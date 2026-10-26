//! Large values in parts (FR-SMUGGLR-078 rev 28): a change whose envelope
//! and values do not fit one datagram carries each large value's hash and
//! size in place of the value, and the value travels as numbered parts, each
//! sealed and naming the value's hash, its number and the count.
//!
//! The maker sends from its outbox's copy of each value, taken from the
//! database a piece at a time when the change was found, unicast to each
//! peer; parts take turns with every other send under the same window and
//! quarter. A receiver writes each part into smugglr's own files as it
//! arrives, never holding a value whole, and acknowledges the parts it has
//! and names the ones it misses, so only those are resent. It holds the
//! change until every part of every large value has arrived and the whole
//! hashes to the value's hash and the row to the change's `to`; then the
//! change is decided like any other. Half-assembled values have a cap and an
//! expiry, so a peer that stops mid-row cannot fill the disk.
//!
//! How a part carries a value's bytes is [`read_part`] and [`write_part`]
//! alone: the value's own bytes, a text's UTF-8 or a blob's.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use smugglr_core::envelope::Envelope;
use smugglr_core::hex::hex;
use smugglr_core::large::{self, Large};
use smugglr_core::row_hash::RowHasher;

use crate::wire::{Message, RANGES, Range};

/// The bytes of a value one part carries, so a sealed part stays within
/// [`crate::wire::DATAGRAM`].
pub(crate) const PART: usize = 672;

/// The most bytes of half-assembled values a node keeps; parts of a further
/// value are let go until room is made. Current choice 2026-10-26: 2 GB,
/// two values of SQLite's largest; revisable.
const CAP: u64 = 2_000_000_000;

/// A half-assembled value no part of which has arrived for this long is let
/// go. Current choice 2026-10-26: 10 minutes; revisable.
const EXPIRY: Duration = Duration::from_secs(600);

/// A half-assembled value no part of which has arrived for this long names
/// every part it misses, so a lost tail or a maker that restarted resends.
const STALLED: Duration = Duration::from_secs(1);

/// While a peer acknowledged parts of a change this recently, the change's
/// envelope is not resent to it: its parts show it has the envelope coming.
const BUSY: Duration = Duration::from_secs(1);

/// The shortest wait before a part is resent again.
const AGAIN: Duration = Duration::from_millis(50);

/// The bytes read from a file at once.
const PIECE: usize = 1 << 20;

/// How many parts `large` travels in.
pub(crate) fn count(large: &Large) -> u64 {
    large.size.div_ceil(PART as u64)
}

/// Part `n` of a value whose own bytes are in `file`, as a part carries it.
fn read_part(file: &mut File, n: u64) -> std::io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(n * PART as u64))?;
    let mut data = Vec::with_capacity(PART);
    file.take(PART as u64).read_to_end(&mut data)?;
    Ok(data)
}

/// Writes part `n`, as a part carries it, into `file`, which holds the
/// value's own bytes.
fn write_part(file: &mut File, n: u64, data: &[u8]) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(n * PART as u64))?;
    file.write_all(data)
}

/// Each large value `envelope` carries, by its column.
pub(crate) fn large_values(envelope: &Envelope) -> Vec<(usize, Large)> {
    envelope
        .change
        .values
        .iter()
        .enumerate()
        .filter_map(|(i, value)| Large::parse(value).map(|large| (i, large)))
        .collect()
}

/// Part numbers as a bit each.
#[derive(Debug, Default)]
struct Bits(Vec<u64>);

impl Bits {
    fn get(&self, n: u64) -> bool {
        let n = usize::try_from(n).unwrap_or(usize::MAX);
        self.0.get(n / 64).is_some_and(|word| word & (1 << (n % 64)) != 0)
    }

    /// Sets `n`; whether it was not set.
    fn set(&mut self, n: u64) -> bool {
        let n = usize::try_from(n).unwrap_or(usize::MAX);
        if self.0.len() <= n / 64 {
            self.0.resize(n / 64 + 1, 0);
        }
        let new = self.0[n / 64] & (1 << (n % 64)) == 0;
        self.0[n / 64] |= 1 << (n % 64);
        new
    }

    /// Up to `most` ranges of the numbers below `end` not set.
    fn gaps(&self, end: u64, most: usize) -> Vec<Range> {
        let mut gaps: Vec<Range> = Vec::new();
        let mut n = 0;
        while n < end && gaps.len() < most {
            let at = usize::try_from(n).unwrap_or(usize::MAX);
            if n % 64 == 0 && self.0.get(at / 64) == Some(&u64::MAX) {
                n += 64;
                continue;
            }
            if !self.get(n) {
                match gaps.last_mut() {
                    Some((_, last)) if *last == n => *last = n + 1,
                    _ => gaps.push((n, n + 1)),
                }
            }
            n += 1;
        }
        gaps
    }
}

/// `numbers`, sorted, as ranges.
fn ranges(mut numbers: Vec<u64>) -> Vec<Range> {
    numbers.sort_unstable();
    numbers.dedup();
    let mut out: Vec<Range> = Vec::new();
    for n in numbers {
        match out.last_mut() {
            Some((_, end)) if *end == n => *end = n + 1,
            _ => out.push((n, n + 1)),
        }
    }
    out
}

/// One part to send: to whom, and the part.
#[derive(Debug)]
pub(crate) struct Piece {
    pub(crate) peer: String,
    pub(crate) value: [u8; 32],
    pub(crate) n: u64,
    pub(crate) count: u64,
    pub(crate) sunset: u64,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct Flight {
    sent: Instant,
    resent: bool,
}

/// How one peer stands with one value the node sends.
#[derive(Debug)]
struct PeerOut {
    /// The next part never sent to it.
    next: u64,
    acked: Bits,
    acked_count: u64,
    /// Parts sent and not acknowledged, and when each last went.
    flying: BTreeMap<u64, Flight>,
    /// Parts it named missing.
    nacked: BTreeSet<u64>,
    /// When it last acknowledged a part, or was first sent one.
    since: Option<Instant>,
}

#[derive(Debug)]
struct Out {
    change: [u8; 32],
    large: Large,
    path: PathBuf,
    file: Option<File>,
    count: u64,
    sunset: u64,
    peers: BTreeMap<String, PeerOut>,
}

impl Out {
    fn read(&mut self, n: u64) -> std::io::Result<Vec<u8>> {
        if self.file.is_none() {
            self.file = Some(File::open(&self.path)?);
        }
        match &mut self.file {
            Some(file) => read_part(file, n),
            None => Ok(Vec::new()),
        }
    }
}

fn peer_out() -> PeerOut {
    PeerOut {
        next: 0,
        acked: Bits::default(),
        acked_count: 0,
        flying: BTreeMap::new(),
        nacked: BTreeSet::new(),
        since: None,
    }
}

/// The large values of the node's own changes it sends, in the order their
/// changes were first sent, and how each peer stands with each.
#[derive(Debug, Default)]
pub(crate) struct Sending {
    outs: Vec<Out>,
}

impl Sending {
    /// The large values of `envelope`, just first sent, go to `peers`; each
    /// value's own bytes are in the file `path` names for its column.
    pub(crate) fn start(
        &mut self,
        envelope: &Envelope,
        peers: &BTreeSet<String>,
        path: impl Fn(usize) -> PathBuf,
    ) {
        for (column, large) in large_values(envelope) {
            let path = path(column);
            if self
                .outs
                .iter()
                .any(|out| out.change == envelope.signature && out.path == path)
            {
                continue;
            }
            self.outs.push(Out {
                change: envelope.signature,
                large,
                path,
                file: None,
                count: count(&large),
                sunset: envelope.sunset,
                peers: peers.iter().map(|peer| (peer.clone(), peer_out())).collect(),
            });
        }
    }

    /// A peer heard for the first time: every value still sent goes to it.
    pub(crate) fn heard(&mut self, peer: &str) {
        for out in &mut self.outs {
            out.peers.entry(peer.to_owned()).or_insert_with(peer_out);
        }
    }

    /// Keeps only the peers each value is still owed to, as `owes` says of
    /// its change, and the values owed to someone.
    pub(crate) fn retain(&mut self, owes: impl Fn(&[u8; 32], &str) -> bool) {
        for out in &mut self.outs {
            let change = out.change;
            out.peers.retain(|peer, _| owes(&change, peer));
        }
        self.outs.retain(|out| !out.peers.is_empty());
    }

    /// Parts sent and not yet acknowledged.
    pub(crate) fn in_flight(&self) -> usize {
        self.outs
            .iter()
            .flat_map(|out| out.peers.values())
            .map(|peer| peer.flying.len())
            .sum()
    }

    /// Whether `peer` is acknowledging parts of `change` still, so its
    /// envelope need not be resent to it.
    pub(crate) fn busy(&self, change: &[u8; 32], peer: &str, now: Instant) -> bool {
        self.outs.iter().any(|out| {
            out.change == *change
                && out.peers.get(peer).is_some_and(|state| {
                    state.acked_count < out.count
                        && state
                            .since
                            .is_some_and(|since| now.duration_since(since) < BUSY)
                })
        })
    }

    /// Up to `limit` parts never sent, oldest value first.
    pub(crate) fn firsts(&mut self, limit: usize, now: Instant) -> std::io::Result<Vec<Piece>> {
        let mut pieces = Vec::new();
        for out in &mut self.outs {
            let names: Vec<String> = out.peers.keys().cloned().collect();
            for peer in names {
                loop {
                    if pieces.len() >= limit {
                        return Ok(pieces);
                    }
                    let Some(state) = out.peers.get_mut(&peer) else {
                        break;
                    };
                    if state.next >= out.count {
                        break;
                    }
                    let n = state.next;
                    state.next += 1;
                    if state.acked.get(n) {
                        continue;
                    }
                    state.flying.insert(
                        n,
                        Flight {
                            sent: now,
                            resent: false,
                        },
                    );
                    state.since.get_or_insert(now);
                    let data = out.read(n)?;
                    pieces.push(Piece {
                        peer: peer.clone(),
                        value: out.large.hash,
                        n,
                        count: out.count,
                        sunset: out.sunset,
                        data,
                    });
                }
            }
        }
        Ok(pieces)
    }

    /// Up to `limit` parts to resend at `now`: first those a peer named
    /// missing, then those unacknowledged past `wait(peer)`, a peer's wait
    /// before a resend (`None` for a peer that has stopped acknowledging,
    /// to which only parts it names missing are resent).
    pub(crate) fn resends(
        &mut self,
        now: Instant,
        wait: impl Fn(&str) -> Option<Duration>,
        limit: usize,
    ) -> std::io::Result<Vec<Piece>> {
        // (sure, the value's place, the peer, the part)
        let mut due: Vec<(bool, usize, String, u64)> = Vec::new();
        for (at, out) in self.outs.iter().enumerate() {
            for (peer, state) in &out.peers {
                let wait = wait(peer);
                for &n in &state.nacked {
                    let again = state
                        .flying
                        .get(&n)
                        .is_none_or(|flight| now >= flight.sent + wait.unwrap_or(AGAIN).min(AGAIN * 4));
                    if again && !state.acked.get(n) {
                        due.push((true, at, peer.clone(), n));
                    }
                }
                if let Some(wait) = wait {
                    for (&n, flight) in &state.flying {
                        if now >= flight.sent + wait && !state.nacked.contains(&n) {
                            due.push((false, at, peer.clone(), n));
                        }
                    }
                }
                if due.len() >= limit * 4 {
                    break;
                }
            }
        }
        due.sort_by_key(|(sure, at, _, n)| (!sure, *at, *n));
        due.truncate(limit);
        let mut pieces = Vec::with_capacity(due.len());
        for (_, at, peer, n) in due {
            let Some(out) = self.outs.get_mut(at) else {
                continue;
            };
            if let Some(state) = out.peers.get_mut(&peer) {
                state.nacked.remove(&n);
                state.flying.insert(
                    n,
                    Flight {
                        sent: now,
                        resent: true,
                    },
                );
            }
            let data = out.read(n)?;
            pieces.push(Piece {
                peer,
                value: out.large.hash,
                n,
                count: out.count,
                sunset: out.sunset,
                data,
            });
        }
        Ok(pieces)
    }

    /// `peer` has the parts `have` of the value `value` and misses
    /// `missing`. Gives, for each part acknowledged for the first time, its
    /// round trip (`None` once resent).
    pub(crate) fn ack(
        &mut self,
        peer: &str,
        value: &[u8; 32],
        have: &[Range],
        missing: &[Range],
        now: Instant,
    ) -> Vec<Option<Duration>> {
        let mut acked = Vec::new();
        for out in self.outs.iter_mut().filter(|out| out.large.hash == *value) {
            let count = out.count;
            let Some(state) = out.peers.get_mut(peer) else {
                continue;
            };
            state.since = Some(now);
            for &(start, end) in have {
                for n in start..end.min(count) {
                    let flight = state.flying.remove(&n);
                    state.nacked.remove(&n);
                    if state.acked.set(n) {
                        state.acked_count += 1;
                        acked.push(
                            flight
                                .filter(|flight| !flight.resent)
                                .map(|flight| now.duration_since(flight.sent)),
                        );
                    }
                }
            }
            for &(start, end) in missing {
                // A part never sent yet is not missing; it is coming.
                for n in start..end.min(state.next) {
                    if !state.acked.get(n) {
                        state.nacked.insert(n);
                    }
                }
            }
        }
        acked
    }
}

/// A value being assembled from its parts.
#[derive(Debug)]
struct In {
    maker: String,
    count: u64,
    got: Bits,
    received: u64,
    sunset: u64,
    last: Instant,
    /// Parts arrived since the last acknowledgement.
    fresh: Vec<u64>,
    /// The highest part arrived.
    high: Option<u64>,
    /// The next acknowledgement names every part missing, not only those
    /// below the highest arrived.
    nack_all: bool,
    path: PathBuf,
    file: File,
}

impl In {
    fn complete(&self) -> bool {
        self.received >= self.count
    }
}

/// The large values a node assembles from their parts, and the changes that
/// wait for them.
#[derive(Debug)]
pub(crate) struct Receiving {
    folder: PathBuf,
    ins: HashMap<[u8; 32], In>,
    waiting: HashMap<[u8; 32], Envelope>,
    verified: HashSet<[u8; 32]>,
    /// The values the record has named let go, so each is named once.
    named: HashSet<[u8; 32]>,
}

impl Receiving {
    /// Values assembled in `folder`, in smugglr's own files; any left from a
    /// last run are let go, and their makers resend what is named missing.
    pub(crate) fn open(folder: PathBuf) -> Self {
        let _ = std::fs::remove_dir_all(&folder);
        Self {
            folder,
            ins: HashMap::new(),
            waiting: HashMap::new(),
            verified: HashSet::new(),
            named: HashSet::new(),
        }
    }

    fn bytes(&self) -> u64 {
        self.ins.values().map(|value| value.count * PART as u64).sum()
    }

    /// The value `value`, of `count` parts, from `maker`: assembled already,
    /// or begun now; `None` when there is no room for it under the cap or
    /// its file cannot be made.
    fn value(
        &mut self,
        value: &[u8; 32],
        maker: &str,
        count: u64,
        sunset: u64,
    ) -> Option<&mut In> {
        if !self.ins.contains_key(value) {
            if self.bytes() + count * PART as u64 > CAP {
                if self.named.insert(*value) {
                    println!(
                        "smugglr mesh: let go the parts of a large value from {maker}: \
                         {} bytes of values are already half-assembled",
                        CAP
                    );
                }
                return None;
            }
            let path = self.folder.join(hex(value));
            let file = std::fs::create_dir_all(&self.folder)
                .and_then(|()| {
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&path)
                })
                .map_err(|err| eprintln!("smugglr mesh: {}: {err}", path.display()))
                .ok()?;
            self.ins.insert(
                *value,
                In {
                    maker: maker.to_owned(),
                    count,
                    got: Bits::default(),
                    received: 0,
                    sunset,
                    last: Instant::now(),
                    fresh: Vec::new(),
                    high: None,
                    nack_all: false,
                    path,
                    file,
                },
            );
        }
        self.ins.get_mut(value)
    }

    /// Takes in part `n` of `count` of the value `value`, made by `maker`.
    pub(crate) fn part(
        &mut self,
        maker: &str,
        value: &[u8; 32],
        (n, count, sunset): (u64, u64, u64),
        data: &[u8],
    ) {
        if n >= count {
            return;
        }
        let Some(assembling) = self.value(value, maker, count, sunset) else {
            return;
        };
        assembling.last = Instant::now();
        // A part already here is acknowledged again: its acknowledgement
        // may have been lost.
        assembling.fresh.push(n);
        if assembling.got.get(n) {
            return;
        }
        match write_part(&mut assembling.file, n, data) {
            Ok(()) => {
                assembling.got.set(n);
                assembling.received += 1;
                assembling.high = Some(assembling.high.map_or(n, |high| high.max(n)));
            }
            Err(err) => {
                assembling.fresh.pop();
                eprintln!("smugglr mesh: {}: {err}", assembling.path.display());
            }
        }
    }

    /// Holds `envelope`, a change with large values, until they have all
    /// arrived; a value not yet begun is begun, and the next acknowledgement
    /// names every part it misses.
    pub(crate) fn hold(&mut self, envelope: &Envelope) {
        for (_, large) in large_values(envelope) {
            if let Some(value) = self.value(
                &large.hash,
                &envelope.node,
                count(&large),
                envelope.sunset,
            ) && !value.complete()
            {
                value.nack_all = true;
            }
        }
        self.waiting
            .entry(envelope.signature)
            .or_insert_with(|| envelope.clone());
    }

    /// Whether `change` waits here for its large values.
    pub(crate) fn holds(&self, change: &[u8; 32]) -> bool {
        self.waiting.contains_key(change)
    }

    /// The changes whose large values have all arrived and check: each
    /// value hashes to its hash, and the row to the change's `to`. A value
    /// that does not check is let go, to be sent again.
    pub(crate) fn ready(&mut self) -> Vec<Envelope> {
        let complete: Vec<[u8; 32]> = self
            .waiting
            .iter()
            .filter(|(_, envelope)| {
                large_values(envelope).iter().all(|(_, large)| {
                    self.ins.get(&large.hash).is_some_and(In::complete)
                })
            })
            .map(|(change, _)| *change)
            .collect();
        let mut ready = Vec::new();
        for change in complete {
            let Some(envelope) = self.waiting.get(&change).cloned() else {
                continue;
            };
            let mut good = true;
            for (_, large) in large_values(&envelope) {
                let path = self.ins.get(&large.hash).map(|value| value.path.clone());
                if path.is_none_or(|path| !value_checks(&large, &path)) {
                    println!(
                        "smugglr mesh: a large value of a change to {} {:?} from {} did not \
                         hash to its hash; it is let go and sent again",
                        envelope.change.table, envelope.change.key, envelope.node
                    );
                    self.let_go(&large.hash);
                    good = false;
                }
            }
            if !good {
                continue;
            }
            self.waiting.remove(&change);
            if self.row_checks(&envelope) {
                self.verified.insert(change);
                ready.push(envelope);
            } else {
                println!(
                    "smugglr mesh: refused a change to {} {:?} from {}: its values do not \
                     hash to its to",
                    envelope.change.table, envelope.change.key, envelope.node
                );
                self.done(&envelope);
            }
        }
        ready
    }

    /// Whether `envelope`'s row, its large values read from their files,
    /// hashes to its `to`.
    fn row_checks(&self, envelope: &Envelope) -> bool {
        let mut hasher = RowHasher::new();
        for value in &envelope.change.values {
            match Large::parse(value) {
                Some(large) => {
                    let Some(path) = self.ins.get(&large.hash).map(|value| &value.path) else {
                        return false;
                    };
                    if quoted_into(&large, path, &mut hasher).is_err() {
                        return false;
                    }
                }
                None => {
                    hasher.length(value.len() as u64);
                    hasher.piece(value.as_bytes());
                }
            }
        }
        Some(hasher.finish()) == envelope.change.to
    }

    /// Whether `change`'s large values have arrived and checked.
    pub(crate) fn verified(&self, change: &[u8; 32]) -> bool {
        self.verified.contains(change)
    }

    /// The file that holds the value `large`'s own bytes, once assembled.
    pub(crate) fn path(&self, large: &Large) -> Option<PathBuf> {
        self.ins
            .get(&large.hash)
            .filter(|value| value.complete())
            .map(|value| value.path.clone())
    }

    /// `envelope` is decided: its values' files go, unless another change
    /// still waits for one.
    pub(crate) fn done(&mut self, envelope: &Envelope) {
        self.verified.remove(&envelope.signature);
        self.waiting.remove(&envelope.signature);
        for (_, large) in large_values(envelope) {
            let wanted = self
                .waiting
                .values()
                .any(|other| large_values(other).iter().any(|(_, l)| l.hash == large.hash));
            if !wanted {
                self.let_go(&large.hash);
            }
        }
    }

    /// The changes that wait here for parts of the row `table` `key` moved
    /// past by an applied change from `from`: a change to that row whose
    /// `to` is `from` is had, so its half-assembled values go.
    pub(crate) fn moved_past(&mut self, table: &str, key: &[String], from: Option<[u8; 16]>) -> Vec<Envelope> {
        let past: Vec<Envelope> = self
            .waiting
            .values()
            .filter(|waiting| {
                waiting.change.table == table
                    && waiting.change.key == key
                    && waiting.change.to.is_some()
                    && waiting.change.to == from
            })
            .cloned()
            .collect();
        for envelope in &past {
            self.done(envelope);
        }
        past
    }

    fn let_go(&mut self, value: &[u8; 32]) {
        if let Some(value) = self.ins.remove(value) {
            let _ = std::fs::remove_file(&value.path);
        }
    }

    /// The acknowledgements due, from `node`, each with the maker it goes
    /// to: the parts arrived since the last, and the parts missing below the
    /// highest arrived (every part missing, for a value whose change is
    /// held, or whose parts have stalled).
    pub(crate) fn acks(&mut self, node: &str) -> Vec<(String, Message)> {
        let mut acks = Vec::new();
        for (value, assembling) in &mut self.ins {
            if !assembling.complete() && assembling.last.elapsed() >= STALLED {
                assembling.nack_all = true;
                assembling.last = Instant::now();
            }
            if assembling.fresh.is_empty() && !assembling.nack_all {
                continue;
            }
            let end = if assembling.nack_all {
                assembling.count
            } else {
                assembling.high.map_or(0, |high| high + 1)
            };
            let mut missing = assembling.got.gaps(end, RANGES);
            assembling.nack_all = false;
            let have = ranges(std::mem::take(&mut assembling.fresh));
            let mut chunks: Vec<Vec<Range>> = have.chunks(RANGES).map(<[Range]>::to_vec).collect();
            if chunks.is_empty() {
                chunks.push(Vec::new());
            }
            for have in chunks {
                acks.push((
                    assembling.maker.clone(),
                    Message::Parts {
                        sender: node.to_owned(),
                        maker: assembling.maker.clone(),
                        value: *value,
                        have,
                        missing: std::mem::take(&mut missing),
                    },
                ));
            }
        }
        acks
    }

    /// Lets go each value past its sunset at `now` (seconds since the Unix
    /// epoch) or idle past [`EXPIRY`], and each change still waiting past its
    /// sunset, which the record names.
    pub(crate) fn tidy(&mut self, now: u64) {
        let gone: Vec<[u8; 32]> = self
            .ins
            .iter()
            .filter(|(_, value)| {
                now > value.sunset || (!value.complete() && value.last.elapsed() >= EXPIRY)
            })
            .map(|(value, _)| *value)
            .collect();
        for value in &gone {
            self.let_go(value);
        }
        let late: Vec<Envelope> = self
            .waiting
            .values()
            .filter(|envelope| now > envelope.sunset)
            .cloned()
            .collect();
        for envelope in late {
            println!(
                "smugglr mesh: let go a change to {} {:?} from {}: its large values did not \
                 all arrive by its sunset",
                envelope.change.table, envelope.change.key, envelope.node
            );
            self.done(&envelope);
        }
    }
}

/// Whether the file at `path` holds the value `large`: its size and hash.
fn value_checks(large: &Large, path: &Path) -> bool {
    let mut hasher = large::Hasher::new(large.blob);
    let size = each_piece(path, |piece| hasher.update(piece));
    size.is_ok_and(|size| size == large.size) && hasher.finish() == large.hash
}

/// Feeds the `quote()` text of the value `large`, whose own bytes are in the
/// file at `path`, to `hasher` as one column of a row.
fn quoted_into(large: &Large, path: &Path, hasher: &mut RowHasher) -> std::io::Result<()> {
    let mut quotes = 0_u64;
    if !large.blob {
        each_piece(path, |piece| {
            quotes += piece.iter().filter(|&&byte| byte == b'\'').count() as u64;
        })?;
    }
    hasher.length(large::quoted_len(large.blob, large.size, quotes));
    hasher.piece(large::opening(large.blob));
    let mut quoted = Vec::new();
    each_piece(path, |piece| {
        quoted.clear();
        large::quote(large.blob, piece, &mut quoted);
        hasher.piece(&quoted);
    })?;
    hasher.piece(b"'");
    Ok(())
}

/// Feeds the file at `path` to `each` a piece at a time; gives its bytes.
fn each_piece(path: &Path, mut each: impl FnMut(&[u8])) -> std::io::Result<u64> {
    let mut file = File::open(path)?;
    let mut piece = vec![0; PIECE];
    let mut size = 0;
    loop {
        let read = file.read(&mut piece)?;
        if read == 0 {
            return Ok(size);
        }
        each(&piece[..read]);
        size += read as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::{Bits, ranges};

    #[test]
    fn gaps_and_ranges_name_part_numbers_in_runs() {
        let mut bits = Bits::default();
        for n in [0, 1, 2, 5, 6, 70] {
            bits.set(n);
        }
        assert_eq!(bits.gaps(8, 16), vec![(3, 5), (7, 8)]);
        assert_eq!(bits.gaps(200, 2), vec![(3, 5), (7, 70)]);
        assert_eq!(ranges(vec![6, 5, 1, 2, 2, 9]), vec![(1, 3), (5, 7), (9, 10)]);
    }
}
