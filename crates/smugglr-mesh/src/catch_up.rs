//! A stash carried over the mesh itself (FR-SMUGGLR-079): the blob's text as
//! numbered sealed parts of one datagram each, paced, the receiver saying what
//! it has and what it misses, and only what it misses resent. No new port and
//! no firewall rule, so it works wherever the mesh does. The blob is signed by
//! its maker, so what arrives is checked whole before anything is taken.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::parts::PART;
use crate::wire::{RANGES, Range};

/// How often a node that needs a stash says so.
pub(crate) const WANT_AGAIN: Duration = Duration::from_secs(5);

/// How long a node asks for a stash with no offer before it carries on: no
/// peer is up to date, so none can help.
pub(crate) const NO_OFFER: Duration = Duration::from_secs(60);

/// How often a node repeats that it took up an offer, until the stash is ready.
pub(crate) const ACCEPT_AGAIN: Duration = Duration::from_secs(1);

/// The most changes a node holds back while a stash is on its way; past it the
/// oldest goes, and its maker's resend brings it again.
pub(crate) const HELD_BACK: usize = 20_000;

/// How many stashes in a row may arrive and not be taken before a node stops
/// asking: another would take the same way, and each costs a whole transfer.
pub(crate) const MOST_FAILED: u32 = 3;

/// How often the receiver says what it has.
const REPORT: Duration = Duration::from_millis(100);

/// How often the sender repeats where a stash is, until the receiver answers.
const READY_AGAIN: Duration = Duration::from_secs(1);

/// How long either side waits with nothing heard before it gives up, and the
/// node that wants the stash asks again.
pub(crate) const GIVE_UP: Duration = Duration::from_secs(30);

/// How many parts may be unanswered at first, and at least and at most.
const FIRST_WINDOW: u64 = 64;
const LEAST_WINDOW: u64 = 16;
const MOST_WINDOW: u64 = 8_192;

/// How many parts `bytes` takes.
fn parts_of(bytes: u64) -> u64 {
    bytes.div_ceil(PART as u64).max(1)
}

/// A stash going out to one peer.
#[derive(Debug)]
pub(crate) struct Giving {
    pub(crate) peer: String,
    pub(crate) id: u64,
    bytes: Arc<Vec<u8>>,
    /// The next part never sent.
    next: u64,
    /// Every part below this has arrived, but for `missing`.
    have: u64,
    /// The parts the receiver says it misses, not yet sent again.
    missing: BTreeSet<u64>,
    window: u64,
    answered: bool,
    said_ready: Option<Instant>,
    heard_at: Instant,
}

impl Giving {
    pub(crate) fn new(peer: String, id: u64, bytes: Vec<u8>) -> Self {
        Self {
            peer,
            id,
            bytes: Arc::new(bytes),
            next: 0,
            have: 0,
            missing: BTreeSet::new(),
            window: FIRST_WINDOW,
            answered: false,
            said_ready: None,
            heard_at: Instant::now(),
        }
    }

    pub(crate) fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub(crate) fn count(&self) -> u64 {
        parts_of(self.len())
    }

    /// Whether the receiver should be told again where the stash is.
    pub(crate) fn ready_due(&mut self) -> bool {
        if self.answered || self.said_ready.is_some_and(|at| at.elapsed() < READY_AGAIN) {
            return false;
        }
        self.said_ready = Some(Instant::now());
        true
    }

    /// The bytes of part `n`.
    pub(crate) fn part(&self, n: u64) -> &[u8] {
        let start = usize::try_from(n.saturating_mul(PART as u64)).unwrap_or(usize::MAX);
        let start = start.min(self.bytes.len());
        let end = start.saturating_add(PART).min(self.bytes.len());
        &self.bytes[start..end]
    }

    /// Up to `most` part numbers to send now: what is known missing first,
    /// then new parts while the window has room.
    pub(crate) fn due(&mut self, most: usize) -> Vec<u64> {
        let mut due: Vec<u64> = self.missing.iter().take(most).copied().collect();
        for n in &due {
            self.missing.remove(n);
        }
        let room = self.have.saturating_add(self.window);
        while due.len() < most && self.next < self.count() && self.next < room {
            due.push(self.next);
            self.next += 1;
        }
        due
    }

    /// Takes in what the receiver says: it has everything below `have` but
    /// `missing`. The window grows while nothing is lost and halves when
    /// something is.
    pub(crate) fn heard(&mut self, have: u64, missing: &[Range]) {
        self.answered = true;
        self.heard_at = Instant::now();
        self.have = self.have.max(have);
        self.missing.clear();
        for (start, end) in missing {
            let end = (*end).min(self.next);
            self.missing.extend(*start..end);
        }
        // Everything has gone and nothing is reported missing, yet not all
        // has arrived: the last parts were lost, and the receiver cannot tell
        // parts it never heard of from parts that were never sent.
        if self.missing.is_empty() && self.next >= self.count() && self.have < self.count() {
            self.missing.extend(self.have..self.count());
        }
        self.window = if self.missing.is_empty() {
            self.window.saturating_add(8).min(MOST_WINDOW)
        } else {
            (self.window / 2).max(LEAST_WINDOW)
        };
    }

    /// Whether the receiver has it all.
    pub(crate) fn done(&self) -> bool {
        self.have >= self.count() && self.missing.is_empty()
    }

    pub(crate) fn gone_quiet(&self) -> bool {
        self.heard_at.elapsed() >= GIVE_UP
    }
}

/// A stash coming in from one peer.
#[derive(Debug)]
pub(crate) struct Taking {
    pub(crate) peer: String,
    pub(crate) id: u64,
    count: u64,
    bytes: Vec<u8>,
    arrived: Vec<bool>,
    /// Every part below this has arrived.
    have: u64,
    /// The highest part that has arrived, plus one.
    highest: u64,
    reported: Option<Instant>,
    heard_at: Instant,
}

impl Taking {
    /// A stash of `bytes` in `count` parts; `None` when the two do not agree,
    /// or there is no room for it.
    pub(crate) fn new(peer: String, id: u64, bytes: u64, count: u64) -> Option<Self> {
        if count != parts_of(bytes) {
            return None;
        }
        let size = usize::try_from(bytes).ok()?;
        let parts = usize::try_from(count).ok()?;
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(size).ok()?;
        buffer.resize(size, 0);
        let mut arrived = Vec::new();
        arrived.try_reserve_exact(parts).ok()?;
        arrived.resize(parts, false);
        Some(Self {
            peer,
            id,
            count,
            bytes: buffer,
            arrived,
            have: 0,
            highest: 0,
            reported: None,
            heard_at: Instant::now(),
        })
    }

    /// Takes in part `n`; a part out of range or of the wrong size is dropped.
    pub(crate) fn take(&mut self, n: u64, data: &[u8]) {
        self.heard_at = Instant::now();
        let Ok(at) = usize::try_from(n) else {
            return;
        };
        let start = at.saturating_mul(PART);
        let Some(slot) = self.bytes.get_mut(start..start.saturating_add(data.len())) else {
            return;
        };
        let Some(arrived) = self.arrived.get_mut(at) else {
            return;
        };
        slot.copy_from_slice(data);
        *arrived = true;
        self.highest = self.highest.max(n + 1);
        while self
            .arrived
            .get(usize::try_from(self.have).unwrap_or(usize::MAX))
            .copied()
            .unwrap_or(false)
        {
            self.have += 1;
        }
    }

    /// What to tell the sender, at most every [`REPORT`]: every part below
    /// the first number has arrived, and these are missing above it.
    pub(crate) fn report_due(&mut self) -> Option<(u64, Vec<Range>)> {
        if self.reported.is_some_and(|at| at.elapsed() < REPORT) {
            return None;
        }
        self.reported = Some(Instant::now());
        Some((self.have, self.missing()))
    }

    /// The runs of parts missing below the highest that arrived, at most
    /// [`RANGES`].
    fn missing(&self) -> Vec<Range> {
        let mut ranges = Vec::new();
        let mut n = self.have;
        while n < self.highest && ranges.len() < RANGES {
            if self.arrived[usize::try_from(n).unwrap_or(usize::MAX)] {
                n += 1;
                continue;
            }
            let start = n;
            while n < self.highest && !self.arrived[usize::try_from(n).unwrap_or(usize::MAX)] {
                n += 1;
            }
            ranges.push((start, n));
        }
        ranges
    }

    /// The whole stash, once every part has arrived.
    pub(crate) fn whole(&self) -> Option<&[u8]> {
        (self.have >= self.count).then_some(self.bytes.as_slice())
    }

    pub(crate) fn gone_quiet(&self) -> bool {
        self.heard_at.elapsed() >= GIVE_UP
    }
}

#[cfg(test)]
mod tests {
    use super::{FIRST_WINDOW, Giving, LEAST_WINDOW, PART, Taking};

    fn stash(parts: usize) -> Vec<u8> {
        std::iter::repeat_n(0..=250_u8, parts * PART / 251 + 1)
            .flatten()
            .take(parts * PART)
            .collect()
    }

    fn pair(parts: usize) -> (Giving, Taking) {
        let bytes = stash(parts);
        let giving = Giving::new("sleeper".to_owned(), 7, bytes.clone());
        let taking = Taking::new("giver".to_owned(), 7, giving.len(), giving.count());
        let taking = taking.unwrap_or_else(|| unreachable!("its sizes agree"));
        (giving, taking)
    }

    #[test]
    fn a_stash_whose_sizes_do_not_agree_is_refused() {
        let taking = |bytes, count| Taking::new("giver".to_owned(), 7, bytes, count).is_some();
        assert!(taking(PART as u64 * 2, 2));
        assert!(taking(1, 1));
        assert!(taking(0, 1));
        assert!(!taking(0, 0));
        assert!(!taking(PART as u64 * 2, 3));
        assert!(!taking(u64::MAX, 1));
        assert!(!taking(u64::MAX, u64::MAX));
    }

    #[test]
    fn a_stash_arrives_whole_over_a_clean_path() {
        let (mut giving, mut taking) = pair(500);
        while !giving.done() {
            for n in giving.due(usize::MAX) {
                taking.take(n, giving.part(n));
            }
            giving.heard(taking.have, &taking.missing());
        }
        assert_eq!(taking.whole(), Some(stash(500).as_slice()));
    }

    #[test]
    fn only_what_is_lost_is_sent_again_and_the_stash_still_arrives_whole() {
        let (mut giving, mut taking) = pair(500);
        let mut sent = 0;
        let mut lost = std::collections::BTreeSet::new();
        while !giving.done() {
            for n in giving.due(usize::MAX) {
                sent += 1;
                // One in ten is lost, the first time it goes.
                if n % 10 != 3 || !lost.insert(n) {
                    taking.take(n, giving.part(n));
                }
            }
            giving.heard(taking.have, &taking.missing());
        }
        assert_eq!(taking.whole(), Some(stash(500).as_slice()));
        assert!(sent < 600, "{sent} parts sent for 500");
    }

    #[test]
    fn the_last_parts_lost_are_sent_again() {
        let (mut giving, mut taking) = pair(20);
        for n in giving.due(usize::MAX) {
            if n < 17 {
                taking.take(n, giving.part(n));
            }
        }
        giving.heard(taking.have, &taking.missing());
        assert_eq!(giving.due(usize::MAX), [17, 18, 19]);
    }

    #[test]
    fn the_window_grows_while_nothing_is_lost_and_halves_when_something_is() {
        let (mut giving, _) = pair(10_000);
        giving.next = 10;
        giving.heard(1, &[]);
        assert!(giving.window > FIRST_WINDOW);
        let grown = giving.window;
        giving.heard(1, &[(2, 3)]);
        assert_eq!(giving.window, grown / 2);
        for _ in 0..20 {
            giving.heard(1, &[(2, 3)]);
        }
        assert_eq!(giving.window, LEAST_WINDOW);
    }

    #[test]
    fn nothing_past_the_window_goes_until_the_receiver_answers() {
        let (mut giving, _) = pair(1_000);
        assert_eq!(giving.due(usize::MAX).len() as u64, FIRST_WINDOW);
        assert_eq!(giving.due(usize::MAX).len(), 0);
    }

    #[test]
    fn a_stash_of_nothing_is_one_empty_part() {
        let mut giving = Giving::new("sleeper".to_owned(), 7, Vec::new());
        assert_eq!(giving.count(), 1);
        assert_eq!(giving.due(usize::MAX), [0]);
        assert_eq!(giving.part(0).len(), 0);
    }

    #[test]
    fn a_part_out_of_range_is_dropped_without_harm() {
        let (_, mut taking) = pair(2);
        taking.take(99, &[1; PART]);
        taking.take(u64::MAX, &[1; PART]);
        taking.take(1, &[1; PART * 4]);
        assert_eq!(taking.whole(), None);
        assert_eq!(taking.have, 0);
    }
}
