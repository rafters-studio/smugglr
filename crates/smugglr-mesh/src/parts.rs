//! A change too large for one datagram travels in numbered parts
//! (FR-SMUGGLR-078 rev 31): its message, `quote()` text and all, cut into
//! parts of about 1,200 bytes sealed, each keyed by the change's row hash
//! and its number and naming the count. The maker sends each part unicast to
//! each peer; the peer acknowledges the parts it has and names those it
//! misses, and only those are resent. Once every part has arrived the peer
//! reads the change whole and decides it like any other: its values must
//! hash to its `to` before the row goes live.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use crate::wire::{Id, Message, RANGES, Range};

/// The bytes of a change's message one part carries, so a sealed part stays
/// within [`crate::wire::DATAGRAM`].
pub(crate) const PART: usize = 720;

/// A half-assembled change no part of which has arrived for this long is let
/// go; its maker resends what the peer then names missing. 10 minutes.
const EXPIRY: Duration = Duration::from_secs(600);

/// A half-assembled change no part of which has arrived for this long names
/// every part it misses, so a lost tail is resent.
const STALLED: Duration = Duration::from_secs(1);

/// While a peer acknowledged parts of a change this recently, the change is
/// not resent to it whole: its parts are on their way.
const BUSY: Duration = Duration::from_secs(1);

/// The shortest wait before a part named missing is resent again.
const AGAIN: Duration = Duration::from_millis(50);

/// Part numbers as a bit each.
#[derive(Debug, Default)]
struct Bits(Vec<u64>);

impl Bits {
    fn index(n: u64) -> (usize, u64) {
        let n = usize::try_from(n).unwrap_or(usize::MAX);
        (n / 64, 1 << (n % 64))
    }

    fn get(&self, n: u64) -> bool {
        let (word, bit) = Self::index(n);
        self.0.get(word).is_some_and(|w| w & bit != 0)
    }

    /// Sets `n`; whether it was not set.
    fn set(&mut self, n: u64) -> bool {
        let (word, bit) = Self::index(n);
        if self.0.len() <= word {
            self.0.resize(word + 1, 0);
        }
        let new = self.0[word] & bit == 0;
        self.0[word] |= bit;
        new
    }

    /// Clears `n`; whether it was set.
    fn clear(&mut self, n: u64) -> bool {
        let (word, bit) = Self::index(n);
        let Some(w) = self.0.get_mut(word) else {
            return false;
        };
        let was = *w & bit != 0;
        *w &= !bit;
        was
    }

    /// Up to `most` ranges of the numbers below `end` not set.
    fn gaps(&self, end: u64, most: usize) -> Vec<Range> {
        let mut gaps: Vec<Range> = Vec::new();
        let mut n = 0;
        while n < end {
            let (word, _) = Self::index(n);
            if n % 64 == 0 && self.0.get(word) == Some(&u64::MAX) {
                n += 64;
                continue;
            }
            if !self.get(n) {
                let full = gaps.len() >= most;
                match gaps.last_mut() {
                    Some((_, last)) if *last == n => *last = n + 1,
                    _ if full => break,
                    _ => gaps.push((n, n + 1)),
                }
            }
            n += 1;
        }
        gaps
    }
}

/// `numbers` as sorted ranges.
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

/// How many parts a message of `len` bytes travels in.
fn count(len: usize) -> u64 {
    len.div_ceil(PART) as u64
}

/// One part to send, and to whom.
#[derive(Debug)]
pub(crate) struct Piece {
    pub(crate) peer: String,
    pub(crate) id: Id,
    pub(crate) n: u64,
    pub(crate) count: u64,
    pub(crate) data: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
struct Flight {
    sent: Instant,
    resent: bool,
}

/// How one peer stands with one change sent in parts.
#[derive(Debug, Default)]
struct PeerOut {
    /// The next part never sent to it.
    next: u64,
    acked: Bits,
    acked_count: u64,
    /// Parts sent and not acknowledged, and when each last went.
    flying: BTreeMap<u64, Flight>,
    /// Parts it named missing.
    missing: BTreeSet<u64>,
    /// When it last acknowledged a part, or was first sent one.
    since: Option<Instant>,
}

#[derive(Debug)]
struct Out {
    change: [u8; 32],
    id: Id,
    message: Vec<u8>,
    count: u64,
    peers: BTreeMap<String, PeerOut>,
}

impl Out {
    fn piece(&self, peer: String, n: u64) -> Piece {
        let start = usize::try_from(n)
            .unwrap_or(usize::MAX)
            .saturating_mul(PART);
        let start = start.min(self.message.len());
        let end = start.saturating_add(PART).min(self.message.len());
        Piece {
            peer,
            id: self.id,
            n,
            count: self.count,
            data: self.message[start..end].to_vec(),
        }
    }
}

/// The node's own changes it sends in parts, in the order they were first
/// sent, and how each peer stands with each.
#[derive(Debug, Default)]
pub(crate) struct Sending {
    outs: Vec<Out>,
}

impl Sending {
    /// The change `change`, whose `from` and `to` are `id` and whose
    /// message is `message`, goes in parts to `peers`.
    pub(crate) fn start(
        &mut self,
        change: [u8; 32],
        id: Id,
        message: Vec<u8>,
        peers: &BTreeSet<String>,
    ) {
        if self.outs.iter().any(|out| out.change == change) {
            return;
        }
        self.outs.push(Out {
            change,
            id,
            count: count(message.len()),
            message,
            peers: peers
                .iter()
                .map(|peer| (peer.clone(), PeerOut::default()))
                .collect(),
        });
    }

    /// Whether `change` is being sent in parts.
    pub(crate) fn sends(&self, change: &[u8; 32]) -> bool {
        self.outs.iter().any(|out| out.change == *change)
    }

    /// A peer heard for the first time: every change still sent goes to it.
    pub(crate) fn heard(&mut self, peer: &str) {
        for out in &mut self.outs {
            out.peers.entry(peer.to_owned()).or_default();
        }
    }

    /// Keeps only the peers each change is still owed to, as `owes` says,
    /// and the changes owed to someone.
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

    /// Whether `peer` is still acknowledging parts of `change`, so it is
    /// not resent whole.
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

    /// `change` is due a resend to `peer`, which has not acknowledged it:
    /// every part it has not acknowledged is named missing, and when it has
    /// acknowledged every part, it lost them, so every part is.
    pub(crate) fn again(&mut self, change: &[u8; 32], peer: &str) {
        for out in self.outs.iter_mut().filter(|out| out.change == *change) {
            let count = out.count;
            let state = out.peers.entry(peer.to_owned()).or_default();
            if state.acked_count >= count {
                state.acked = Bits::default();
                state.acked_count = 0;
            }
            for n in 0..state.next {
                if !state.acked.get(n) {
                    state.missing.insert(n);
                }
            }
        }
    }

    /// `peer` refuses the change keyed by `id` for its size: it is sent no
    /// more of its parts. Gives the changes it names.
    pub(crate) fn refused(&mut self, peer: &str, id: &Id) -> Vec<[u8; 32]> {
        let mut changes = Vec::new();
        for out in self.outs.iter_mut().filter(|out| out.id == *id) {
            if out.peers.remove(peer).is_some() {
                changes.push(out.change);
            }
        }
        changes
    }

    /// Up to `limit` parts never sent, the oldest change first.
    pub(crate) fn firsts(&mut self, limit: usize, now: Instant) -> Vec<Piece> {
        let mut pieces = Vec::new();
        for out in &mut self.outs {
            let mut due = Vec::new();
            for (peer, state) in &mut out.peers {
                while pieces.len() + due.len() < limit && state.next < out.count {
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
                    due.push((peer.clone(), n));
                }
            }
            pieces.extend(due.into_iter().map(|(peer, n)| out.piece(peer, n)));
            if pieces.len() >= limit {
                break;
            }
        }
        pieces
    }

    /// Up to `limit` parts to resend at `now`: first those a peer named
    /// missing, then those unacknowledged past `wait(peer)`, a peer's wait
    /// before a resend (`None` for a peer that has stopped acknowledging, to
    /// which only parts it names missing are resent).
    pub(crate) fn resends(
        &mut self,
        now: Instant,
        wait: impl Fn(&str) -> Option<Duration>,
        limit: usize,
    ) -> Vec<Piece> {
        // (named missing, the change's place, the peer, the part)
        let mut due: Vec<(bool, usize, String, u64)> = Vec::new();
        for (at, out) in self.outs.iter().enumerate() {
            for (peer, state) in &out.peers {
                let wait = wait(peer);
                for &n in &state.missing {
                    let again = state
                        .flying
                        .get(&n)
                        .is_none_or(|flight| now >= flight.sent + AGAIN);
                    if again {
                        due.push((true, at, peer.clone(), n));
                    }
                }
                if let Some(wait) = wait {
                    for (&n, flight) in &state.flying {
                        if now >= flight.sent + wait && !state.missing.contains(&n) {
                            due.push((false, at, peer.clone(), n));
                        }
                    }
                }
            }
            if due.len() >= limit {
                break;
            }
        }
        due.sort_by_key(|(missing, at, _, n)| (!missing, *at, *n));
        due.truncate(limit);
        let mut pieces = Vec::with_capacity(due.len());
        for (_, at, peer, n) in due {
            let Some(out) = self.outs.get_mut(at) else {
                continue;
            };
            if let Some(state) = out.peers.get_mut(&peer) {
                state.missing.remove(&n);
                state.flying.insert(
                    n,
                    Flight {
                        sent: now,
                        resent: true,
                    },
                );
            }
            pieces.push(out.piece(peer, n));
        }
        pieces
    }

    /// `peer` has the parts `have` of the change keyed by `id` and misses
    /// `missing`; a part it names missing is resent even if it was
    /// acknowledged, since a peer that started again lost it. Gives, for
    /// each part acknowledged for the first time, its round trip (`None`
    /// once resent).
    pub(crate) fn ack(
        &mut self,
        peer: &str,
        id: &Id,
        have: &[Range],
        missing: &[Range],
        now: Instant,
    ) -> Vec<Option<Duration>> {
        let mut acked = Vec::new();
        for out in self.outs.iter_mut().filter(|out| out.id == *id) {
            let count = out.count;
            let Some(state) = out.peers.get_mut(peer) else {
                continue;
            };
            state.since = Some(now);
            for &(start, end) in have {
                for n in start..end.min(count) {
                    let flight = state.flying.remove(&n);
                    state.missing.remove(&n);
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
                // A part never sent is not missing; it is coming.
                for n in start..end.min(state.next) {
                    if state.acked.clear(n) {
                        state.acked_count -= 1;
                    }
                    state.missing.insert(n);
                }
            }
        }
        acked
    }
}

/// A change being assembled from its parts.
#[derive(Debug)]
struct In {
    count: u64,
    got: Bits,
    received: u64,
    message: Vec<u8>,
    last: Instant,
    /// Parts arrived since the last acknowledgement.
    fresh: Vec<u64>,
    /// The highest part arrived.
    high: u64,
    /// The next acknowledgement names every part missing, not only those
    /// below the highest arrived.
    every: bool,
}

/// The changes a node assembles from their parts, by maker and change.
#[derive(Debug, Default)]
pub(crate) struct Receiving {
    /// The most bytes a change's message may take here; a change of more
    /// parts is let go before anything is kept for it.
    most: u64,
    /// The changes let go for their size, so each is named once.
    too_large: std::collections::HashSet<(String, Id)>,
    ins: HashMap<(String, Id), In>,
    /// Changes refused for their size whose makers are due word of it.
    refusals: Vec<(String, Id)>,
    /// Changes assembled lately, so a late copy of a part is acknowledged,
    /// not begun again.
    done: HashMap<(String, Id), Instant>,
    /// Acknowledgements of parts of changes already assembled.
    late: Vec<(String, Id, u64)>,
}

impl Receiving {
    /// Assembles changes whose values are at most `limit` bytes, the node's
    /// value limit: a message of up to four times that, a blob's hex for
    /// about two such values, and a mebibyte besides for the rest of the
    /// row and the envelope.
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            most: limit.saturating_mul(4).saturating_add(1 << 20),
            ..Self::default()
        }
    }

    /// Takes in part `n` of `count` of the change keyed by `id` from
    /// `maker`; the change's message, once every part has arrived.
    pub(crate) fn part(
        &mut self,
        maker: &str,
        id: Id,
        (n, count): (u64, u64),
        data: &[u8],
    ) -> Option<Vec<u8>> {
        if n >= count || data.len() > PART {
            return None;
        }
        let key = (maker.to_owned(), id);
        if count.saturating_mul(PART as u64) > self.most {
            // Refused as it would be once assembled, and its maker told, so
            // its parts no longer hold the maker's window.
            if self.too_large.insert(key.clone()) {
                println!(
                    "smugglr mesh: refused a change from {maker} in {count} parts: more than \
                     the {} bytes a change may take here",
                    self.most
                );
            }
            if !self.refusals.contains(&key) {
                self.refusals.push(key);
            }
            return None;
        }
        if self.done.contains_key(&key) {
            self.late.push((maker.to_owned(), id, n));
            return None;
        }
        let assembling = self.ins.entry(key.clone()).or_insert_with(|| In {
            count,
            got: Bits::default(),
            received: 0,
            message: Vec::new(),
            last: Instant::now(),
            fresh: Vec::new(),
            high: 0,
            every: false,
        });
        if assembling.count != count {
            return None;
        }
        assembling.last = Instant::now();
        // A part already here is acknowledged again: its acknowledgement
        // may have been lost.
        assembling.fresh.push(n);
        if !assembling.got.set(n) {
            return None;
        }
        let start = usize::try_from(n)
            .unwrap_or(usize::MAX)
            .saturating_mul(PART);
        let end = start.saturating_add(data.len());
        if assembling.message.len() < end {
            assembling.message.resize(end, 0);
        }
        assembling.message[start..end].copy_from_slice(data);
        assembling.received += 1;
        assembling.high = assembling.high.max(n);
        if assembling.received < assembling.count {
            return None;
        }
        let whole = self.ins.remove(&key)?;
        self.late
            .extend(whole.fresh.iter().map(|&n| (maker.to_owned(), id, n)));
        self.done.insert(key, Instant::now());
        Some(whole.message)
    }

    /// The acknowledgements due, from `node`, each with the maker it goes
    /// to: the parts arrived since the last, and the parts missing below the
    /// highest arrived (every part missing, for a change whose parts have
    /// stalled).
    pub(crate) fn acks(&mut self, node: &str) -> Vec<(String, Message)> {
        let mut acks: Vec<(String, Message)> = self
            .refusals
            .drain(..)
            .map(|(maker, id)| {
                let refused = Message::Refused {
                    sender: node.to_owned(),
                    maker: maker.clone(),
                    id,
                };
                (maker, refused)
            })
            .collect();
        let mut late: BTreeMap<(String, Id), Vec<u64>> = BTreeMap::new();
        for (maker, id, n) in self.late.drain(..) {
            late.entry((maker, id)).or_default().push(n);
        }
        for ((maker, id), numbers) in late {
            for have in ranges(numbers).chunks(RANGES) {
                acks.push((
                    maker.clone(),
                    Message::Parts {
                        sender: node.to_owned(),
                        maker: maker.clone(),
                        id,
                        have: have.to_vec(),
                        missing: Vec::new(),
                    },
                ));
            }
        }
        for ((maker, id), assembling) in &mut self.ins {
            if assembling.last.elapsed() >= STALLED {
                assembling.every = true;
                assembling.last = Instant::now();
            }
            if assembling.fresh.is_empty() && !assembling.every {
                continue;
            }
            let end = if assembling.every {
                assembling.count
            } else {
                assembling.high
            };
            assembling.every = false;
            let mut missing = assembling.got.gaps(end, RANGES);
            let have = ranges(std::mem::take(&mut assembling.fresh));
            let mut chunks: Vec<&[Range]> = have.chunks(RANGES).collect();
            if chunks.is_empty() {
                chunks.push(&[]);
            }
            for have in chunks {
                acks.push((
                    maker.clone(),
                    Message::Parts {
                        sender: node.to_owned(),
                        maker: maker.clone(),
                        id: *id,
                        have: have.to_vec(),
                        missing: std::mem::take(&mut missing),
                    },
                ));
            }
        }
        acks
    }

    /// Lets go each change no part of which has arrived for [`EXPIRY`].
    pub(crate) fn tidy(&mut self) {
        self.ins.retain(|_, value| value.last.elapsed() < EXPIRY);
        self.done.retain(|_, at| at.elapsed() < EXPIRY);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Instant;

    use super::{Bits, PART, Receiving, Sending, ranges};

    #[test]
    fn gaps_and_ranges_name_part_numbers_in_runs() {
        let mut bits = Bits::default();
        for n in [0, 1, 2, 5, 6, 70] {
            bits.set(n);
        }
        assert_eq!(bits.gaps(8, 16), vec![(3, 5), (7, 8)]);
        assert_eq!(bits.gaps(200, 2), vec![(3, 5), (7, 70)]);
        assert_eq!(
            ranges(vec![6, 5, 1, 2, 2, 9]),
            vec![(1, 3), (5, 7), (9, 10)]
        );
    }

    #[test]
    fn a_message_crosses_in_parts_and_only_a_missing_part_is_resent() {
        let message: Vec<u8> = (0..PART * 3 + 10)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect();
        let mut sending = Sending::default();
        sending.start(
            [1; 32],
            (None, [2; 16]),
            message.clone(),
            &BTreeSet::from(["b".to_owned()]),
        );
        let now = Instant::now();
        let pieces = sending.firsts(100, now);
        assert_eq!(pieces.len(), 4);
        let mut receiving = Receiving::new(1_000);
        // Part 1 is lost.
        for piece in pieces.iter().filter(|piece| piece.n != 1) {
            assert_eq!(
                receiving.part("a", piece.id, (piece.n, piece.count), &piece.data),
                None
            );
        }
        let acks = receiving.acks("b");
        for (_, ack) in &acks {
            if let crate::wire::Message::Parts {
                id, have, missing, ..
            } = ack
            {
                assert_eq!(missing, &vec![(1, 2)]);
                sending.ack("b", id, have, missing, now);
            }
        }
        let resent = sending.resends(now + super::AGAIN, |_| None, 100);
        assert_eq!(
            resent.iter().map(|piece| piece.n).collect::<Vec<_>>(),
            vec![1]
        );
        let piece = &resent[0];
        assert_eq!(
            receiving.part("a", piece.id, (piece.n, piece.count), &piece.data),
            Some(message)
        );
        // A change of more parts than the node's limit allows is let go
        // before anything is kept for it.
        let mut small = Receiving::new(0);
        let parts = (1 << 20) / PART as u64 + 2;
        assert_eq!(small.part("a", (None, [3; 16]), (0, parts), &[1]), None);
        assert!(small.ins.is_empty());
        // Its maker is told it is refused, and sends it no more.
        let mut big = Sending::default();
        big.start(
            [4; 32],
            (None, [3; 16]),
            vec![0; PART * 2],
            &BTreeSet::from(["b".to_owned()]),
        );
        assert_eq!(big.firsts(10, now).len(), 2);
        let told = small.acks("b");
        assert!(
            matches!(told.as_slice(), [(maker, super::Message::Refused { .. })] if maker == "a")
        );
        assert_eq!(big.refused("b", &(None, [3; 16])), vec![[4; 32]]);
        assert_eq!(big.in_flight(), 0);
    }
}
