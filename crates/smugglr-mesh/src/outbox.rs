//! The outbox: the node's own changes that a peer it has heard has not yet
//! acknowledged, kept in smugglr's own files so they survive a restart, and
//! read into memory a segment at a time, never all at once.
//!
//! Each segment file holds up to [`SEGMENT`] changes, one per line with the
//! targets it was made for (the peers heard by then), and is written once;
//! an acknowledgement is one line appended to the acks file. A peer heard
//! later becomes a target of every change still owed.
//! A segment is removed once none of its changes is still owed, and the
//! acks file once the outbox is empty. What the outbox weighs is the bytes
//! of its files.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::{hex, unhex};
use smugglr_memory::Store;

use crate::Error;

/// The most envelopes one segment file holds.
const SEGMENT: usize = 1_000;

/// How many segments are kept read into memory at once.
const CACHED: usize = 8;

/// The first resend's wait to a peer that has stopped acknowledging; each
/// later one doubles, up to [`LAST_RESEND`] (toy Z, for sleepers).
const FIRST_RESEND: Duration = Duration::from_secs(10);

/// A peer that has acknowledged nothing for this long has stopped
/// acknowledging.
const SILENT: Duration = Duration::from_secs(10);

/// How many changes sent after one a peer must acknowledge before the one
/// it skipped is resent at once, so a change merely reordered is not.
const SKIPPED: u64 = 3;

/// The shortest wait for a resend to an acknowledging peer.
const SHORTEST: Duration = Duration::from_millis(50);

/// The acks file is rewritten only once it holds at least this many bytes.
const COMPACT: u64 = 64 * 1024;

/// The longest wait between two resends.
const LAST_RESEND: Duration = Duration::from_secs(160);

/// A change due a resend: to which peers, and whether to a silent peer (on
/// the doubling).
#[derive(Debug)]
struct Due {
    seq: u64,
    change: [u8; 32],
    to: Vec<String>,
    silent: bool,
}

/// A change due a resend, and the targets it is due to.
pub(crate) type Owed = (Envelope, Vec<String>);

#[derive(Debug)]
struct Entry {
    segment: u64,
    sunset: u64,
    /// The peers it is owed to.
    targets: BTreeSet<String>,
    /// Targets heard after its first send, which have had no copy yet.
    unsent_to: BTreeSet<String>,
    /// The peers that have acknowledged it.
    acked: BTreeSet<String>,
    sent: bool,
    /// The change's place in the order of first sends.
    seq: u64,
    sent_at: Instant,
    /// When it last went out, first send or resend.
    last_sent: Instant,
    /// For each peer it has been resent to, the last first send by then: a
    /// peer that acknowledges changes sent after that has skipped the resend
    /// too, so it is resent at once again.
    resent_by: HashMap<String, u64>,
    /// It has been resent, so an acknowledgement times no round trip.
    resent: bool,
    /// When it is next resent to a peer that has stopped acknowledging.
    resend_at: Instant,
    interval: Duration,
}

/// How a peer has been acknowledging this node's changes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timing {
    /// Its smoothed round trip.
    pub(crate) round_trip: Duration,
    /// How far its round trips stray from the smoothed one (RFC 6298).
    pub(crate) variation: Duration,
    pub(crate) last_ack: Instant,
    /// The latest change, in the order of first sends, it has acknowledged.
    pub(crate) highest: u64,
}

impl Timing {
    fn acknowledging(&self, now: Instant) -> bool {
        now.duration_since(self.last_ack) < SILENT
    }

    /// The wait before a first resend: about two round trips, longer when
    /// round trips vary (RFC 6298).
    fn wait(&self) -> Duration {
        (self.round_trip * 2)
            .max(self.round_trip + self.variation * 4)
            .max(SHORTEST)
    }

    /// Takes in one round trip measured.
    pub(crate) fn measure(&mut self, sample: Duration) {
        let gap = self.round_trip.abs_diff(sample);
        self.variation = (self.variation * 3 + gap) / 4;
        self.round_trip = (self.round_trip * 7 + sample) / 8;
    }
}

/// What an acknowledgement tells the node.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Acked {
    /// The peer is the first to acknowledge the change.
    pub(crate) first: bool,
    /// The change's place in the order of first sends.
    pub(crate) seq: u64,
    /// The round trip, from its one send to now; `None` once resent.
    pub(crate) round_trip: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct Outbox {
    store: Arc<dyn Store>,
    folder: String,
    next: u64,
    entries: HashMap<[u8; 32], Entry>,
    unsent: VecDeque<[u8; 32]>,
    /// The last first send's place in the order.
    seq: u64,
    /// Each live segment's bytes and how many of its changes are still owed.
    segments: BTreeMap<u64, (u64, usize)>,
    cache: HashMap<u64, HashMap<[u8; 32], Envelope>>,
    acks_bytes: u64,
    acks: String,
    index_bytes: u64,
    /// A change has been acknowledged by every target since the last prune.
    settled: bool,
}

impl Outbox {
    /// The outbox kept in `folder`, less what is past its sunset at `now`.
    pub(crate) fn open(store: Arc<dyn Store>, folder: &str, now: u64) -> Result<Self, Error> {
        let mut outbox = Self {
            store,
            folder: folder.to_owned(),
            next: 0,
            entries: HashMap::new(),
            unsent: VecDeque::new(),
            seq: 0,
            segments: BTreeMap::new(),
            cache: HashMap::new(),
            acks_bytes: 0,
            acks: String::new(),
            index_bytes: 0,
            settled: false,
        };
        let index: Vec<u64> = outbox
            .store
            .read(&outbox.index())?
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        for segment in index {
            let Some(text) = outbox.store.read(&outbox.segment(segment))? else {
                continue;
            };
            let mut live = 0;
            for line in text.lines() {
                let Some((envelope, targets)) = read_line(line) else {
                    continue;
                };
                if now > envelope.sunset {
                    continue;
                }
                live += 1;
                outbox.unsent.push_back(envelope.signature);
                outbox
                    .entries
                    .insert(envelope.signature, entry(segment, &envelope, targets));
            }
            outbox.segments.insert(segment, (text.len() as u64, live));
            outbox.next = outbox.next.max(segment + 1);
        }
        let acks = outbox.store.read(&outbox.acks_file())?.unwrap_or_default();
        let mut kept = String::new();
        for line in acks.lines() {
            let parsed = serde_json::from_str::<Value>(line).ok().and_then(|value| {
                let change: [u8; 32] = unhex(value.get("c")?.as_str()?)?.try_into().ok()?;
                Some((change, value.get("n")?.as_str()?.to_owned()))
            });
            if let Some((change, node)) = parsed
                && let Some(entry) = outbox.entries.get_mut(&change)
            {
                entry.acked.insert(node);
                kept.push_str(line);
                kept.push('\n');
            }
        }
        outbox.store.replace(&outbox.acks_file(), &kept)?;
        outbox.acks_bytes = kept.len() as u64;
        outbox.prune(now)?;
        outbox.save_index()?;
        Ok(outbox)
    }

    fn index(&self) -> String {
        format!("{}/segments.json", self.folder)
    }

    fn segment(&self, segment: u64) -> String {
        format!("{}/{segment:020}.jsonl", self.folder)
    }

    fn acks_file(&self) -> String {
        format!("{}/acks.jsonl", self.folder)
    }

    fn save_index(&mut self) -> Result<(), Error> {
        let index: Vec<u64> = self.segments.keys().copied().collect();
        let text = json!(index).to_string();
        self.index_bytes = text.len() as u64;
        Ok(self.store.replace(&self.index(), &text)?)
    }

    /// What `envelope`, owed to `targets`, adds to the outbox's weight.
    pub(crate) fn weigh(envelope: &Envelope, targets: &BTreeSet<String>) -> u64 {
        line(envelope, targets).len() as u64
    }

    /// What the outbox weighs: the bytes of its files.
    pub(crate) fn bytes(&self) -> u64 {
        self.segments.values().map(|(bytes, _)| bytes).sum::<u64>()
            + self.acks_bytes
            + self.acks.len() as u64
            + self.index_bytes
    }

    /// Takes `envelopes` in, to be sent and owed to `targets`.
    pub(crate) fn add(
        &mut self,
        envelopes: &[Envelope],
        targets: &BTreeSet<String>,
    ) -> Result<(), Error> {
        for chunk in envelopes.chunks(SEGMENT) {
            let segment = self.next;
            self.next += 1;
            let mut text = String::new();
            for envelope in chunk {
                text.push_str(&line(envelope, targets));
            }
            self.store.replace(&self.segment(segment), &text)?;
            self.segments
                .insert(segment, (text.len() as u64, chunk.len()));
            let mut cached = HashMap::new();
            for envelope in chunk {
                self.unsent.push_back(envelope.signature);
                self.entries.insert(
                    envelope.signature,
                    entry(segment, envelope, targets.clone()),
                );
                cached.insert(envelope.signature, envelope.clone());
            }
            self.cache(segment, cached);
        }
        self.save_index()
    }

    fn cache(&mut self, segment: u64, envelopes: HashMap<[u8; 32], Envelope>) {
        while self.cache.len() >= CACHED {
            let Some(&oldest) = self.cache.keys().min() else {
                break;
            };
            self.cache.remove(&oldest);
        }
        self.cache.insert(segment, envelopes);
    }

    /// The envelope of the change `change`, read from its segment.
    fn envelope(&mut self, change: &[u8; 32]) -> Result<Option<Envelope>, Error> {
        let Some(segment) = self.entries.get(change).map(|entry| entry.segment) else {
            return Ok(None);
        };
        if !self.cache.contains_key(&segment) {
            let text = self.store.read(&self.segment(segment))?.unwrap_or_default();
            let envelopes = text
                .lines()
                .filter_map(read_line)
                .map(|(envelope, _)| (envelope.signature, envelope))
                .collect();
            self.cache(segment, envelopes);
        }
        Ok(self
            .cache
            .get(&segment)
            .and_then(|envelopes| envelopes.get(change))
            .cloned())
    }

    /// How many changes wait for their first send.
    pub(crate) fn unsent(&self) -> usize {
        self.unsent.len()
    }

    /// How many changes have been sent and acknowledged by no peer yet.
    pub(crate) fn in_flight(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.sent && entry.acked.is_empty())
            .count()
    }

    /// Up to `limit` changes not yet sent, oldest first, now counted sent.
    pub(crate) fn first_sends(&mut self, limit: usize) -> Result<Vec<Envelope>, Error> {
        let mut out = Vec::new();
        while out.len() < limit {
            let Some(change) = self.unsent.pop_front() else {
                break;
            };
            let Some(envelope) = self.envelope(&change)? else {
                continue;
            };
            if let Some(entry) = self.entries.get_mut(&change) {
                let now = Instant::now();
                self.seq += 1;
                entry.sent = true;
                entry.seq = self.seq;
                entry.sent_at = now;
                entry.last_sent = now;
                entry.resend_at = now + entry.interval;
            }
            out.push(envelope);
        }
        Ok(out)
    }

    /// Puts `envelopes`, taken by [`Outbox::first_sends`] and not sent, back
    /// at the front.
    pub(crate) fn unsend(&mut self, envelopes: &[Envelope]) {
        for envelope in envelopes.iter().rev() {
            if let Some(entry) = self.entries.get_mut(&envelope.signature) {
                entry.sent = false;
                self.unsent.push_front(envelope.signature);
            }
        }
    }

    /// A peer heard for the first time: a target of every change still owed.
    pub(crate) fn heard(&mut self, peer: &str) {
        for entry in self.entries.values_mut() {
            if entry.targets.insert(peer.to_owned()) && entry.sent {
                entry.unsent_to.insert(peer.to_owned());
            }
        }
    }

    /// Up to `limit` changes due a resend at `now`, each with the targets
    /// it is due to (FR-SMUGGLR-078 rev 20): at once to a peer that has
    /// acknowledged changes sent after it; to a peer that is acknowledging,
    /// after about two of its round trips, doubling with each try; and to a
    /// peer that has stopped acknowledging, on the 10 s to 160 s doubling.
    pub(crate) fn resends(
        &mut self,
        now: Instant,
        timing: &HashMap<String, Timing>,
        limit: usize,
    ) -> Result<Vec<Owed>, Error> {
        // Which peers each change is due to, and how; the oldest first, so
        // none waits behind newer ones.
        let mut due: Vec<Due> = Vec::new();
        for (change, entry) in &self.entries {
            if !entry.sent {
                continue;
            }
            let (mut to, mut silent) = (Vec::new(), false);
            for peer in entry.targets.difference(&entry.acked) {
                if entry.unsent_to.contains(peer) {
                    // Its first copy to a peer heard since the first send.
                    to.push(peer.clone());
                    continue;
                }
                match timing.get(peer).filter(|timing| timing.acknowledging(now)) {
                    Some(timing)
                        if timing.highest
                            > entry.resent_by.get(peer).copied().unwrap_or(entry.seq) + SKIPPED =>
                    {
                        to.push(peer.clone());
                    }
                    Some(timing) => {
                        // Every two round trips, never doubling: the
                        // doubling is only for a peer that has stopped
                        // (FR-SMUGGLR-078 rev 20); the quarter cap is
                        // what keeps resends from crowding out the rest.
                        if now >= entry.last_sent + timing.wait() {
                            to.push(peer.clone());
                        }
                    }
                    None => {
                        if now >= entry.resend_at {
                            silent = true;
                            to.push(peer.clone());
                        }
                    }
                }
            }
            if !to.is_empty() {
                due.push(Due {
                    seq: entry.seq,
                    change: *change,
                    to,
                    silent,
                });
            }
        }
        due.sort_unstable_by_key(|due| due.seq);
        due.truncate(limit);
        let latest = self.seq;
        for Due {
            change, to, silent, ..
        } in &due
        {
            let Some(entry) = self.entries.get_mut(change) else {
                continue;
            };
            if *silent {
                entry.interval = (entry.interval * 2).min(LAST_RESEND);
                entry.resend_at = now + entry.interval;
            }
            entry.last_sent = now;
            entry.resent = true;
            for peer in to {
                entry.unsent_to.remove(peer);
                entry.resent_by.insert(peer.clone(), latest);
            }
        }
        let due: Vec<([u8; 32], Vec<String>)> =
            due.into_iter().map(|due| (due.change, due.to)).collect();
        let mut out = Vec::new();
        for (change, to) in due {
            if let Some(envelope) = self.envelope(&change)? {
                out.push((envelope, to));
            }
        }
        Ok(out)
    }

    /// Notes that `peer` acknowledged `change`, and what that tells; `None`
    /// for a change not owed or acknowledged already.
    pub(crate) fn ack(&mut self, change: &[u8; 32], peer: &str) -> Option<Acked> {
        let entry = self.entries.get_mut(change)?;
        if !entry.acked.insert(peer.to_owned()) {
            return None;
        }
        self.acks
            .push_str(&json!({"c": hex(change), "n": peer}).to_string());
        self.acks.push('\n');
        self.settled |= entry.targets.is_subset(&entry.acked);
        Some(Acked {
            first: entry.acked.len() == 1,
            seq: entry.seq,
            round_trip: (!entry.resent).then(|| entry.sent_at.elapsed()),
        })
    }

    /// Whether a change has been acknowledged by every target since the last
    /// prune, so a prune would make room.
    pub(crate) fn settled(&self) -> bool {
        self.settled
    }

    /// Forgets each change past its sunset at `now` or acknowledged by every
    /// target, removing the files that no longer hold an owed change, and
    /// writes the acknowledgements noted since the last time.
    pub(crate) fn prune(&mut self, now: u64) -> Result<(), Error> {
        self.settled = false;
        let done: Vec<[u8; 32]> = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                now > entry.sunset
                    || (!entry.targets.is_empty() && entry.targets.is_subset(&entry.acked))
            })
            .map(|(change, _)| *change)
            .collect();
        for change in done {
            if let Some(entry) = self.entries.remove(&change)
                && let Some((_, live)) = self.segments.get_mut(&entry.segment)
            {
                *live = live.saturating_sub(1);
            }
        }
        let empty: Vec<u64> = self
            .segments
            .iter()
            .filter(|(_, (_, live))| *live == 0)
            .map(|(segment, _)| *segment)
            .collect();
        for segment in &empty {
            self.segments.remove(segment);
            self.cache.remove(segment);
            self.store.remove(&self.segment(*segment))?;
        }
        if !empty.is_empty() {
            self.save_index()?;
        }
        if self.entries.is_empty() {
            self.unsent.clear();
            self.acks.clear();
            if self.acks_bytes > 0 {
                self.store.replace(&self.acks_file(), "")?;
                self.acks_bytes = 0;
            }
        } else if !self.acks.is_empty() {
            self.store.append(&self.acks_file(), &self.acks)?;
            self.acks_bytes += self.acks.len() as u64;
            self.acks.clear();
            self.compact_acks()?;
        }
        Ok(())
    }

    /// Rewrites the acks file with only the acknowledgements of changes
    /// still owed, once most of it names changes already gone, so a steady
    /// flow never grows it past the backpressure line.
    fn compact_acks(&mut self) -> Result<(), Error> {
        let live: usize = self.entries.values().map(|entry| entry.acked.len()).sum();
        // An acknowledgement line is about 90 bytes.
        if self.acks_bytes < COMPACT || self.acks_bytes < (live as u64) * 90 * 2 {
            return Ok(());
        }
        let mut text = String::new();
        for (change, entry) in &self.entries {
            for peer in &entry.acked {
                text.push_str(&json!({"c": hex(change), "n": peer}).to_string());
                text.push('\n');
            }
        }
        self.store.replace(&self.acks_file(), &text)?;
        self.acks_bytes = text.len() as u64;
        Ok(())
    }
}

/// The segment line for `envelope`, owed to `targets`.
fn line(envelope: &Envelope, targets: &BTreeSet<String>) -> String {
    let mut line = json!({"e": envelope.to_json(), "t": targets}).to_string();
    line.push('\n');
    line
}

/// A segment line's envelope and the targets it was made for.
fn read_line(line: &str) -> Option<(Envelope, BTreeSet<String>)> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    let envelope = Envelope::from_json(value.get("e")?)?;
    let targets = value
        .get("t")?
        .as_array()?
        .iter()
        .filter_map(|target| target.as_str().map(str::to_owned))
        .collect();
    Some((envelope, targets))
}

fn entry(segment: u64, envelope: &Envelope, targets: BTreeSet<String>) -> Entry {
    Entry {
        segment,
        sunset: envelope.sunset,
        targets,
        unsent_to: BTreeSet::new(),
        acked: BTreeSet::new(),
        sent: false,
        seq: 0,
        sent_at: Instant::now(),
        last_sent: Instant::now(),
        resent_by: HashMap::new(),
        resent: false,
        resend_at: Instant::now(),
        interval: FIRST_RESEND,
    }
}
