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

/// How long the outbox remembers a change it sent, to tell the node's own
/// from a change under its name that another machine made.
const MADE_FOR: Duration = Duration::from_secs(60);

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

/// The longest wait for a resend to an acknowledging peer, so a lost copy
/// is repaired well within a second on a local network (FR-SMUGGLR-078).
const LONGEST: Duration = Duration::from_millis(300);

/// A change sent this long ago, to peers still being heard, is taken as held
/// and leaves the window: a peer that is merely slow answers well within it.
const HELD_PAST: Duration = Duration::from_secs(30);

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
    /// The copy is known lost, or its peer has had none: not a guess.
    sure: bool,
    /// The peers it is due to on a guess that their copy was lost: each is
    /// acknowledging other changes.
    guessed: Vec<String>,
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
    /// For each peer it has been resent to, the last first send by then: a
    /// peer that acknowledges changes sent after that has skipped the resend
    /// too, so it is resent at once again.
    resent_by: HashMap<String, u64>,
    /// It has been resent, so an acknowledgement times no round trip.
    resent: bool,
    /// When it is next resent to a peer that has stopped acknowledging.
    resend_at: Instant,
    interval: Duration,
    /// About the bytes it takes in its segment.
    bytes: u64,
    /// Its last target was taken off the mesh, so it can go.
    owes_nobody: bool,
    /// For each peer that is acknowledging other changes, how many resends
    /// have gone to it on a guess and when the last went: each one doubles
    /// the wait before the next, for that peer alone.
    guesses: HashMap<String, (u32, Instant)>,
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
    pub(crate) fn acknowledging(&self, now: Instant) -> bool {
        now.duration_since(self.last_ack) < SILENT
    }

    /// The wait before a resend: about two round trips, longer when round
    /// trips vary (RFC 6298), but never past [`LONGEST`]: on a busy machine
    /// the measured round trips swell with queueing, and an unbounded wait
    /// left a lost copy unrepaired for seconds while its peer kept
    /// acknowledging.
    pub(crate) fn wait(&self) -> Duration {
        (self.round_trip * 2)
            .max(self.round_trip + self.variation * 4)
            .clamp(SHORTEST, LONGEST)
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
    /// The changes found past their sunset at the start, not acknowledged by
    /// the peers named, as the record says them (toy X).
    expired: BTreeMap<String, Vec<String>>,
    /// The changes sent or resent lately, which may yet come back to the
    /// node, each with when it last went.
    made: HashMap<[u8; 32], Instant>,
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
            expired: BTreeMap::new(),
            made: HashMap::new(),
        };
        let mut expired: HashMap<[u8; 32], (BTreeSet<String>, String)> = HashMap::new();
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
                    let said = format!(
                        "{} {:?} (change {})",
                        envelope.change.table,
                        envelope.change.key,
                        hex(&envelope.signature)
                    );
                    expired.insert(envelope.signature, (targets, said));
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
            if let Some((change, node)) = parsed {
                if let Some((targets, _)) = expired.get_mut(&change) {
                    // Kept while its segment is on disk, so the next start
                    // does not name a peer that did acknowledge it.
                    targets.remove(&node);
                    kept.push_str(line);
                    kept.push('\n');
                }
                if let Some(entry) = outbox.entries.get_mut(&change) {
                    entry.acked.insert(node);
                    kept.push_str(line);
                    kept.push('\n');
                }
            }
        }
        for (targets, said) in expired.into_values() {
            for peer in targets {
                outbox.expired.entry(peer).or_default().push(said.clone());
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

    /// The changes found past their sunset at the start and not acknowledged,
    /// by peer, once.
    pub(crate) fn take_expired(&mut self) -> BTreeMap<String, Vec<String>> {
        std::mem::take(&mut self.expired)
    }

    /// Every change still owed to a peer (to `only`, when named), for a
    /// wazzup to ask about.
    pub(crate) fn owed_changes(&mut self, only: Option<&str>) -> Result<Vec<Envelope>, Error> {
        let mut changes: Vec<([u8; 32], u64)> = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                only.map_or_else(
                    || !entry.targets.is_subset(&entry.acked),
                    |peer| entry.targets.contains(peer) && !entry.acked.contains(peer),
                )
            })
            .map(|(change, entry)| (*change, entry.seq))
            .collect();
        changes.sort_unstable_by_key(|(_, seq)| *seq);
        let mut out = Vec::with_capacity(changes.len());
        for (change, _) in changes {
            if let Some(envelope) = self.envelope(&change)? {
                out.push(envelope);
            }
        }
        Ok(out)
    }

    /// About the bytes of the changes owed to `peer`, to see whether they are
    /// too much to answer a wazzup with.
    pub(crate) fn owed_bytes(&self, peer: &str) -> u64 {
        self.entries
            .values()
            .filter(|entry| entry.targets.contains(peer) && !entry.acked.contains(peer))
            .map(|entry| entry.bytes)
            .sum()
    }

    /// `change` is due to `peer` at once: a wazzup showed the peer needs it.
    pub(crate) fn due_now(&mut self, change: &[u8; 32], peer: &str) {
        if let Some(entry) = self.entries.get_mut(change)
            && entry.sent
            && entry.targets.contains(peer)
            && !entry.acked.contains(peer)
        {
            entry.unsent_to.insert(peer.to_owned());
        }
    }

    /// Everything owed to `peer` is due at once: it woke and asked. A change
    /// `in_parts` is left out, since its parts have their own repair.
    pub(crate) fn all_due_now(&mut self, peer: &str, in_parts: impl Fn(&[u8; 32]) -> bool) {
        for (change, entry) in &mut self.entries {
            if entry.sent
                && entry.targets.contains(peer)
                && !entry.acked.contains(peer)
                && !in_parts(change)
            {
                entry.unsent_to.insert(peer.to_owned());
            }
        }
    }

    /// `peer` is gone for good: nothing is owed to it any more.
    pub(crate) fn remove_target(&mut self, peer: &str) {
        for entry in self.entries.values_mut() {
            let held = entry.targets.remove(peer);
            entry.unsent_to.remove(peer);
            entry.acked.remove(peer);
            entry.guesses.remove(peer);
            entry.resent_by.remove(peer);
            // Only a change it was owed: one made before any peer was heard
            // still waits for its first target.
            entry.owes_nobody |= held && entry.targets.is_empty();
        }
        self.settled = true;
    }

    /// Whether `change` is one the outbox holds.
    pub(crate) fn holds(&self, change: &[u8; 32]) -> bool {
        self.entries.contains_key(change)
    }

    /// Whether the node made `change`, as far as it can tell at `now`: the
    /// outbox holds it, or sent it lately.
    pub(crate) fn made(&self, change: &[u8; 32], now: Instant) -> bool {
        self.holds(change)
            || self
                .made
                .get(change)
                .is_some_and(|at| now.saturating_duration_since(*at) < MADE_FOR)
    }

    /// Forgets the changes sent longer ago than [`MADE_FOR`] at `now`.
    pub(crate) fn forget_made(&mut self, now: Instant) {
        self.made
            .retain(|_, at| now.saturating_duration_since(*at) < MADE_FOR);
    }

    /// Whether `change` is still owed to `peer`.
    pub(crate) fn owes(&self, change: &[u8; 32], peer: &str) -> bool {
        self.entries
            .get(change)
            .is_some_and(|entry| entry.targets.contains(peer) && !entry.acked.contains(peer))
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

    /// How many changes have been sent and acknowledged by no peer yet, less
    /// those past [`HELD_PAST`] whose every target `heard` says is still
    /// being heard: those peers are holding them, so changes behind them can
    /// go. A sleeper is not heard, so its changes go on counting.
    pub(crate) fn in_flight(&self, now: Instant, heard: impl Fn(&str) -> bool) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.sent && entry.acked.is_empty())
            .filter(|entry| {
                let held = now.duration_since(entry.sent_at) >= HELD_PAST
                    && !entry.targets.is_empty()
                    && entry.targets.iter().all(|peer| heard(peer));
                !held
            })
            .count()
    }

    /// How many sent changes `peer` still owes an acknowledgement, and when
    /// the oldest of them went out; `None` when it owes none.
    pub(crate) fn owed(&self, peer: &str) -> Option<(usize, Instant)> {
        let mut owed = self.entries.values().filter(|entry| {
            entry.sent && entry.targets.contains(peer) && !entry.acked.contains(peer)
        });
        let oldest = owed.next()?.sent_at;
        let (count, oldest) = owed.fold((1, oldest), |(count, oldest), entry| {
            (count + 1, oldest.min(entry.sent_at))
        });
        Some((count, oldest))
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
                entry.resend_at = now + entry.interval;
                self.made.insert(change, now);
            }
            out.push(envelope);
        }
        Ok(out)
    }

    /// Lets `change` go without its acknowledgements: it can never be sent.
    pub(crate) fn forget(&mut self, change: &[u8; 32]) {
        if let Some(entry) = self.entries.remove(change)
            && let Some((_, live)) = self.segments.get_mut(&entry.segment)
        {
            *live = live.saturating_sub(1);
            self.settled = true;
        }
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
    /// it is due to: at once to a peer that has acknowledged changes sent
    /// after it; to a peer that is acknowledging, after about two of its
    /// round trips, doubling with each try up to [`LAST_RESEND`]; and to a
    /// peer that has stopped acknowledging, on the 10 s to 160 s doubling.
    /// A peer that `busy` says is taking in a change's parts is not due
    /// that change.
    pub(crate) fn resends(
        &mut self,
        now: Instant,
        timing: &HashMap<String, Timing>,
        limit: usize,
        busy: impl Fn(&[u8; 32], &str) -> bool,
    ) -> Result<Vec<Owed>, Error> {
        // Which peers each change is due to, and how; the oldest first, so
        // none waits behind newer ones.
        let mut due: Vec<Due> = Vec::new();
        for (change, entry) in &self.entries {
            if !entry.sent {
                continue;
            }
            let (mut to, mut silent, mut sure) = (Vec::new(), false, false);
            let mut guessed = Vec::new();
            for peer in entry.targets.difference(&entry.acked) {
                if busy(change, peer) {
                    continue;
                }
                if entry.unsent_to.contains(peer) {
                    // Its first copy to a peer heard since the first send.
                    to.push(peer.clone());
                    sure = true;
                    continue;
                }
                match timing.get(peer).filter(|timing| timing.acknowledging(now)) {
                    Some(timing)
                        if timing.highest
                            > entry.resent_by.get(peer).copied().unwrap_or(entry.seq) + SKIPPED =>
                    {
                        to.push(peer.clone());
                        sure = true;
                    }
                    Some(timing) => {
                        // A peer that acknowledges others may be holding
                        // this one, and a held change needs no copy to be
                        // decided: each guess waits twice the last.
                        let (tries, since) = entry
                            .guesses
                            .get(peer)
                            .copied()
                            .unwrap_or((0, entry.sent_at));
                        if now >= since + backed_off(timing.wait(), tries) {
                            to.push(peer.clone());
                            guessed.push(peer.clone());
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
                    sure,
                    guessed,
                });
            }
        }
        // A copy known lost (a peer acknowledged what came after it) goes
        // before one whose acknowledgement may only be slow, so a busy
        // machine's guesses never take the quarter from a real loss.
        due.sort_unstable_by_key(|due| (!due.sure, due.seq));
        due.truncate(limit);
        let latest = self.seq;
        for Due {
            change,
            to,
            silent,
            guessed,
            ..
        } in &due
        {
            let Some(entry) = self.entries.get_mut(change) else {
                continue;
            };
            if *silent {
                entry.interval = (entry.interval * 2).min(LAST_RESEND);
                entry.resend_at = now + entry.interval;
            }
            for peer in guessed {
                let guesses = entry.guesses.get(peer).map_or(0, |(guesses, _)| *guesses);
                entry
                    .guesses
                    .insert(peer.clone(), (guesses.saturating_add(1), now));
            }
            entry.resent = true;
            // Its own copy may come back after an acknowledgement lets it go.
            self.made.insert(*change, now);
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
                    || entry.owes_nobody
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
        bytes: line(envelope, &targets).len() as u64,
        owes_nobody: false,
        segment,
        sunset: envelope.sunset,
        targets,
        unsent_to: BTreeSet::new(),
        acked: BTreeSet::new(),
        sent: false,
        seq: 0,
        sent_at: Instant::now(),
        resent_by: HashMap::new(),
        resent: false,
        resend_at: Instant::now(),
        interval: FIRST_RESEND,
        guesses: HashMap::new(),
    }
}

/// `wait` doubled `guesses` times, never past [`LAST_RESEND`].
fn backed_off(wait: Duration, guesses: u32) -> Duration {
    wait.saturating_mul(1_u32.checked_shl(guesses).unwrap_or(u32::MAX))
        .min(LAST_RESEND)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use smugglr_core::change::{Change, op};
    use smugglr_core::envelope::{Envelope, VERSION};
    use smugglr_core::shape::Stamp;
    use smugglr_memory::Files;

    use super::{HELD_PAST, LAST_RESEND, LONGEST, MADE_FOR, Outbox, Timing};

    const PEER: &str = "peer";

    fn envelope(n: u8) -> Envelope {
        let (from, to) = (Some([n; 16]), Some([n + 1; 16]));
        Envelope {
            v: VERSION,
            node: "here".to_owned(),
            change: Change {
                op: op(from, to),
                table: "notes".to_owned(),
                key: vec![n.to_string()],
                from,
                to,
                values: Vec::new(),
            },
            targets: Vec::new(),
            sunset: u64::MAX,
            txn: None,
            key_id: "key".to_owned(),
            stamp: Stamp([0; 16]),
            signature: [n; 32],
        }
    }

    /// An outbox that has sent `count` changes to [`PEER`].
    fn sent(name: &str, count: u8) -> Result<Outbox, Box<dyn std::error::Error>> {
        let dir =
            std::env::temp_dir().join(format!("smugglr-mesh-outbox-{}-{name}", std::process::id()));
        let mut outbox = Outbox::open(Arc::new(Files::new(dir)), "outbox", 0)?;
        let envelopes: Vec<Envelope> = (1..=count).map(envelope).collect();
        outbox.add(&envelopes, &BTreeSet::from([PEER.to_owned()]))?;
        outbox.first_sends(usize::from(count))?;
        Ok(outbox)
    }

    /// [`PEER`] acknowledging others just now, up to the change `highest`.
    fn acknowledging(now: Instant, highest: u64) -> HashMap<String, Timing> {
        HashMap::from([(
            PEER.to_owned(),
            Timing {
                round_trip: Duration::from_secs(1),
                variation: Duration::ZERO,
                last_ack: now,
                highest,
            },
        )])
    }

    #[test]
    fn a_change_resent_to_an_acknowledging_peer_waits_twice_as_long_each_time()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("backs-off", 1)?;
        let mut now = Instant::now();
        let mut wait = LONGEST;
        while wait < LAST_RESEND {
            now += wait;
            let timing = acknowledging(now, 0);
            assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
            wait *= 2;
            let early = now
                + wait
                    .min(LAST_RESEND)
                    .saturating_sub(Duration::from_millis(1));
            let timing = acknowledging(early, 0);
            assert_eq!(outbox.resends(early, &timing, 10, |_, _| false)?.len(), 0);
        }
        // The wait stops growing at the limit.
        now += LAST_RESEND;
        let timing = acknowledging(now, 0);
        assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
        now += LAST_RESEND;
        let timing = acknowledging(now, 0);
        assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
        Ok(())
    }

    #[test]
    fn a_skipped_change_is_resent_at_once_however_far_it_has_backed_off()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("skipped", 1)?;
        let mut now = Instant::now();
        for guess in 0..6 {
            now += LONGEST * 2_u32.pow(guess);
            let timing = acknowledging(now, 0);
            assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
        }
        // The peer has acknowledged changes sent after it: its copy is lost.
        let timing = acknowledging(now, 100);
        let due = outbox.resends(now, &timing, 10, |_, _| false)?;
        assert_eq!(due.len(), 1);
        Ok(())
    }

    #[test]
    fn one_peers_backing_off_does_not_delay_another_peers_first_resend()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("two-peers", 1)?;
        let mut now = Instant::now();
        for guess in 0..6 {
            now += LONGEST * 2_u32.pow(guess);
            let timing = acknowledging(now, 0);
            assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
        }
        outbox.heard("other");
        let mut timing = acknowledging(now, 0);
        timing.insert("other".to_owned(), timing[PEER]);
        // Its first copy, owed since it was heard.
        let due = outbox.resends(now, &timing, 10, |_, _| false)?;
        assert_eq!(due[0].1, ["other"]);
        let at = now + LONGEST;
        let mut timing = acknowledging(at, 0);
        timing.insert("other".to_owned(), timing[PEER]);
        let due = outbox.resends(at, &timing, 10, |_, _| false)?;
        assert_eq!(due[0].1, ["other"]);
        Ok(())
    }

    #[test]
    fn changes_a_heard_peer_has_held_past_the_age_leave_the_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let outbox = sent("held", 4)?;
        let now = Instant::now();
        assert_eq!(outbox.in_flight(now, |_| true), 4);
        let later = now + HELD_PAST;
        assert_eq!(outbox.in_flight(later, |_| true), 0);
        Ok(())
    }

    #[test]
    fn changes_to_a_peer_no_longer_heard_stay_in_the_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let outbox = sent("asleep", 4)?;
        let later = Instant::now() + HELD_PAST;
        assert_eq!(outbox.in_flight(later, |_| false), 4);
        Ok(())
    }

    #[test]
    fn a_change_owed_to_no_peer_yet_stays_in_the_window() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = std::env::temp_dir().join(format!(
            "smugglr-mesh-outbox-{}-no-peer",
            std::process::id()
        ));
        let mut outbox = Outbox::open(Arc::new(Files::new(dir)), "outbox", 0)?;
        outbox.add(&[envelope(1)], &BTreeSet::new())?;
        outbox.first_sends(1)?;
        let later = Instant::now() + HELD_PAST;
        assert_eq!(outbox.in_flight(later, |_| true), 1);
        Ok(())
    }

    #[test]
    fn a_peer_owes_only_the_changes_it_has_not_acknowledged()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("owed", 3)?;
        assert_eq!(outbox.owed(PEER).map(|(count, _)| count), Some(3));
        assert_eq!(outbox.owed("other").map(|(count, _)| count), None);
        outbox.ack(&[1; 32], PEER);
        outbox.ack(&[2; 32], PEER);
        assert_eq!(outbox.owed(PEER).map(|(count, _)| count), Some(1));
        outbox.ack(&[3; 32], PEER);
        assert_eq!(outbox.owed(PEER).map(|(count, _)| count), None);
        Ok(())
    }

    #[test]
    fn taking_a_node_off_lets_go_only_of_what_it_was_owed() -> Result<(), Box<dyn std::error::Error>>
    {
        let dir = std::env::temp_dir().join(format!(
            "smugglr-mesh-outbox-{}-removed",
            std::process::id()
        ));
        let mut outbox = Outbox::open(Arc::new(Files::new(dir)), "outbox", 0)?;
        outbox.add(&[envelope(1)], &BTreeSet::new())?;
        outbox.add(&[envelope(3)], &BTreeSet::from([PEER.to_owned()]))?;
        outbox.first_sends(2)?;
        outbox.remove_target(PEER);
        outbox.prune(0)?;
        assert!(
            outbox.holds(&[1; 32]),
            "no peer yet: it still waits for one"
        );
        assert!(!outbox.holds(&[3; 32]), "owed to nobody now");
        Ok(())
    }

    /// #684: a peer back after minutes away acknowledges a change resent
    /// to it, the outbox lets the change go, and only then does the node's
    /// own copy of that resend come back to it: still its own.
    #[test]
    fn a_change_resent_long_after_its_first_send_is_still_made_here_once_let_go()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("made-resent", 1)?;
        let back = Instant::now() + MADE_FOR * 5;
        assert_eq!(
            outbox
                .resends(back, &HashMap::new(), 10, |_, _| false)?
                .len(),
            1
        );
        outbox.ack(&[1; 32], PEER);
        outbox.prune(0)?;
        assert!(!outbox.holds(&[1; 32]), "acknowledged by every target");
        let heard = back + Duration::from_millis(50);
        assert!(outbox.made(&[1; 32], heard), "its own resend, heard back");
        assert!(!outbox.made(&[9; 32], heard), "never sent here");
        Ok(())
    }

    #[test]
    fn the_first_resend_still_comes_within_the_repair_limit()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut outbox = sent("first", 1)?;
        let now = Instant::now() + LONGEST;
        let timing = acknowledging(now, 0);
        assert_eq!(outbox.resends(now, &timing, 10, |_, _| false)?.len(), 1);
        Ok(())
    }
}
