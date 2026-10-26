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

/// The first resend's wait; each later one doubles, up to [`LAST_RESEND`].
const FIRST_RESEND: Duration = Duration::from_secs(10);

/// The longest wait between two resends.
const LAST_RESEND: Duration = Duration::from_secs(160);

/// A change due a resend, and the targets that have not acknowledged it.
pub(crate) type Owed = (Envelope, Vec<String>);

#[derive(Debug)]
struct Entry {
    segment: u64,
    sunset: u64,
    /// The peers it is owed to.
    targets: BTreeSet<String>,
    /// The peers that have acknowledged it.
    acked: BTreeSet<String>,
    sent: bool,
    resend_at: Instant,
    interval: Duration,
}

#[derive(Debug)]
pub(crate) struct Outbox {
    store: Arc<dyn Store>,
    folder: String,
    next: u64,
    entries: HashMap<[u8; 32], Entry>,
    unsent: VecDeque<[u8; 32]>,
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
                entry.sent = true;
                entry.resend_at = Instant::now() + entry.interval;
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
            entry.targets.insert(peer.to_owned());
        }
    }

    /// Up to `limit` changes due a resend at `now`, each with the targets
    /// that have not acknowledged it, each next due after twice its last
    /// wait; and whether one of them no peer has acknowledged, a sign of
    /// loss.
    pub(crate) fn resends(
        &mut self,
        now: Instant,
        limit: usize,
    ) -> Result<(Vec<Owed>, bool), Error> {
        let mut due: Vec<([u8; 32], bool)> = self
            .entries
            .iter()
            .filter(|(_, entry)| {
                entry.sent && now >= entry.resend_at && !entry.targets.is_subset(&entry.acked)
            })
            .map(|(change, entry)| (*change, entry.acked.is_empty()))
            .collect();
        due.truncate(limit);
        let lost = due.iter().any(|(_, none)| *none);
        let mut out = Vec::new();
        for (change, _) in due {
            let mut owed = Vec::new();
            if let Some(entry) = self.entries.get_mut(&change) {
                entry.interval = (entry.interval * 2).min(LAST_RESEND);
                entry.resend_at = now + entry.interval;
                owed = entry.targets.difference(&entry.acked).cloned().collect();
            }
            if let Some(envelope) = self.envelope(&change)? {
                out.push((envelope, owed));
            }
        }
        Ok((out, lost))
    }

    /// Notes that `peer` acknowledged `change`; true when it is the first
    /// peer to.
    pub(crate) fn ack(&mut self, change: &[u8; 32], peer: &str) -> bool {
        let Some(entry) = self.entries.get_mut(change) else {
            return false;
        };
        if !entry.acked.insert(peer.to_owned()) {
            return false;
        }
        self.acks
            .push_str(&json!({"c": hex(change), "n": peer}).to_string());
        self.acks.push('\n');
        self.settled |= entry.targets.is_subset(&entry.acked);
        entry.acked.len() == 1
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
        }
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
        acked: BTreeSet::new(),
        sent: false,
        resend_at: Instant::now(),
        interval: FIRST_RESEND,
    }
}
