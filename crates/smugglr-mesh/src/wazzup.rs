//! The wazzup's batch check (FR-SMUGGLR-079): a node asks each peer about its
//! own changes that peer has not acknowledged, and settles each from the
//! answer. Only what an answer proves is settled; any other answer stays
//! named, and nothing stops (toys U, V and Z).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, Instant};

use smugglr_core::envelope::Envelope;
use smugglr_core::hex::hex;

use crate::wire::{Line, RowNow};

/// About the most bytes of lines one wazzup part carries, so the part sealed
/// stays under the 1,200 bytes that are never fragmented. Its answer stays
/// there too: an answered line takes about 72 bytes, and an asked line at
/// least about 60.
const PART: usize = 800;

/// How often a part a peer has not answered is said again.
pub(crate) const ASK_AGAIN: Duration = Duration::from_secs(1);

/// How many keys one record line names before it gives only a count.
const NAMED: usize = 10;

/// One change a wazzup asks about, and what the node needs to settle it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Asked {
    pub(crate) signature: [u8; 32],
    pub(crate) from: Option<[u8; 16]>,
    pub(crate) line: Line,
}

impl Asked {
    pub(crate) fn of(envelope: &Envelope) -> Self {
        Self {
            signature: envelope.signature,
            from: envelope.change.from,
            line: Line {
                table: envelope.change.table.clone(),
                key: envelope.change.key.clone(),
                to: envelope.change.to,
            },
        }
    }
}

/// What the node does with a peer's answer about one change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Settled {
    /// The peer has it, or built on it, or holds the row this node holds:
    /// the change owes that peer nothing.
    Has,
    /// The peer sits at the change's `from` and is behind: it is sent the
    /// change.
    Send,
    /// The answer proves nothing: named in the record, and nothing stops.
    Named,
}

/// Settles a change from `from` to `to` against what a peer answered,
/// `mine` being this node's row hash now.
///
/// Holding the row this node holds settles it too, beyond FR-SMUGGLR-079: two
/// nodes that agree have nothing left to send. That covers two more cases: a
/// conflict both sides decided alike, which otherwise stays owed until its
/// sunset; and an insert since deleted here, whose peer holds no row either.
/// Sent to that peer, the insert would bring back a row whose delete is
/// already settled.
pub(crate) fn settle(
    (from, to): (Option<[u8; 16]>, Option<[u8; 16]>),
    mine: Option<[u8; 16]>,
    answer: &RowNow,
) -> Settled {
    if answer.hash == to {
        return Settled::Has;
    }
    // Only a real hash can be built on: the empty `to` of a delete must not
    // match a peer's empty last `from`, or a delete is undone (toy U).
    if to.is_some() && answer.last_from == Some(to) {
        return Settled::Has;
    }
    if answer.hash == mine {
        return Settled::Has;
    }
    // For an insert, behind means it never saw the key: a peer with no row
    // that has moved one before has deleted it, and is not behind.
    if answer.hash == from && (from.is_some() || answer.last_from.is_none()) {
        return Settled::Send;
    }
    Settled::Named
}

/// `asked` in numbered parts of about [`PART`] bytes of lines each; always at
/// least one part, since a wazzup with nothing to ask still says wazzup.
pub(crate) fn parts(asked: Vec<Asked>) -> Vec<Vec<Asked>> {
    let mut parts = vec![Vec::new()];
    let mut bytes = 0;
    for one in asked {
        let size = one.line.size();
        if bytes + size > PART && !parts[parts.len() - 1].is_empty() {
            parts.push(Vec::new());
            bytes = 0;
        }
        bytes += size;
        let last = parts.len() - 1;
        parts[last].push(one);
    }
    parts
}

/// One wazzup under way.
#[derive(Debug)]
pub(crate) struct Asking {
    pub(crate) id: u64,
    pub(crate) parts: Vec<Vec<Asked>>,
    /// The one peer it asks, when it checks on a peer that has gone quiet;
    /// `None` for every peer.
    pub(crate) only: Option<String>,
    /// The parts each peer has answered.
    answered: HashMap<String, BTreeSet<u64>>,
    /// Each peer's named changes, as the record says them.
    named: BTreeMap<String, Vec<String>>,
    pub(crate) last_said: Option<Instant>,
}

impl Asking {
    pub(crate) fn new(id: u64, asked: Vec<Asked>, only: Option<String>) -> Self {
        Self {
            id,
            parts: parts(asked),
            only,
            answered: HashMap::new(),
            named: BTreeMap::new(),
            last_said: None,
        }
    }

    /// Whether this wazzup asks `peer`.
    pub(crate) fn asks(&self, peer: &str) -> bool {
        self.only.as_deref().is_none_or(|only| only == peer)
    }

    /// Whether `peer` has answered any part: it has not said too much.
    pub(crate) fn heard_from(&self, peer: &str) -> bool {
        self.answered
            .get(peer)
            .is_some_and(|parts| !parts.is_empty())
    }

    /// The parts `peer` has not answered.
    pub(crate) fn unanswered(&self, peer: &str) -> Vec<u64> {
        let answered = self.answered.get(peer);
        (0..self.parts.len() as u64)
            .filter(|n| answered.is_none_or(|answered| !answered.contains(n)))
            .collect()
    }

    /// Notes that `peer` answered part `n`, the first time; `false` when it
    /// already had.
    pub(crate) fn answered(&mut self, peer: &str, n: u64) -> bool {
        self.answered.entry(peer.to_owned()).or_default().insert(n)
    }

    /// Notes that `peer` answered every part at once, by saying too much.
    pub(crate) fn answered_all(&mut self, peer: &str) {
        let all = 0..self.parts.len() as u64;
        self.answered
            .entry(peer.to_owned())
            .or_default()
            .extend(all);
    }

    /// Notes a change the answer proved nothing about.
    pub(crate) fn name(&mut self, peer: &str, asked: &Asked) {
        self.named.entry(peer.to_owned()).or_default().push(format!(
            "{} {:?} (change {})",
            asked.line.table,
            asked.line.key,
            hex(&asked.signature)
        ));
    }

    /// The record's line for `peer`, once it has answered every part, and
    /// forgets it; `None` when it has not, or nothing is named.
    pub(crate) fn take_named(&mut self, peer: &str) -> Option<String> {
        if !self.unanswered(peer).is_empty() {
            return None;
        }
        let named = self.named.remove(peer)?;
        Some(named_line(peer, &named))
    }
}

/// The record's one line for the changes `peer` has not acknowledged, grouped
/// by that peer (toy X).
pub(crate) fn named_line(peer: &str, named: &[String]) -> String {
    let shown = named
        .iter()
        .take(NAMED)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let more = named.len().saturating_sub(NAMED);
    let more = if more > 0 {
        format!(" and {more} more")
    } else {
        String::new()
    };
    let changes = if named.len() == 1 {
        "change"
    } else {
        "changes"
    };
    format!(
        "smugglr mesh: {peer} has not acknowledged {} {changes} and its rows settle nothing: {shown}{more}",
        named.len()
    )
}

#[cfg(test)]
mod tests {
    use smugglr_core::envelope::{Envelope, VERSION};

    use super::{Asked, Asking, Settled, named_line, parts, settle};
    use crate::wire::{Line, RowNow};

    const F: Option<[u8; 16]> = Some([1; 16]);
    const T: Option<[u8; 16]> = Some([2; 16]);
    const OTHER: Option<[u8; 16]> = Some([3; 16]);

    #[allow(clippy::option_option)]
    fn answer(hash: Option<[u8; 16]>, last_from: Option<Option<[u8; 16]>>) -> RowNow {
        RowNow { hash, last_from }
    }

    fn asked(n: u8) -> Asked {
        Asked {
            signature: [n; 32],
            from: F,
            line: Line {
                table: "notes".to_owned(),
                key: vec![format!("'row-{n}'")],
                to: T,
            },
        }
    }

    #[test]
    fn a_peer_at_the_change_has_it() {
        assert_eq!(settle((F, T), None, &answer(T, None)), Settled::Has);
    }

    #[test]
    fn a_peer_that_built_on_the_change_has_it() {
        assert_eq!(settle((F, T), None, &answer(OTHER, Some(T))), Settled::Has);
    }

    #[test]
    fn a_peer_holding_the_row_this_node_holds_has_nothing_owed() {
        assert_eq!(settle((F, T), OTHER, &answer(OTHER, Some(F))), Settled::Has);
    }

    #[test]
    fn a_peer_at_the_from_is_sent_the_change() {
        assert_eq!(settle((F, T), None, &answer(F, None)), Settled::Send);
    }

    #[test]
    fn a_peer_that_never_saw_an_insert_is_sent_it() {
        assert_eq!(settle((None, T), T, &answer(None, None)), Settled::Send);
    }

    #[test]
    fn a_peer_that_deleted_the_row_is_not_sent_an_insert_again() {
        assert_eq!(
            settle((None, T), None, &answer(None, Some(T))),
            Settled::Has
        );
        assert_eq!(
            settle((None, T), Some([9; 16]), &answer(None, Some(OTHER))),
            Settled::Named
        );
    }

    #[test]
    fn a_delete_is_not_undone_by_an_empty_last_from() {
        // The peer inserted the row from nothing: its last from is empty,
        // which must not read as having built on this delete's empty to.
        assert_eq!(
            settle((F, None), Some([9; 16]), &answer(OTHER, Some(None))),
            Settled::Named
        );
    }

    #[test]
    fn an_insert_since_deleted_here_is_not_sent_to_a_peer_that_never_saw_it() {
        // Inserted then deleted while the peer slept: its delete is settled
        // by the peer holding no row, so the insert must not bring it back.
        assert_eq!(settle((None, T), None, &answer(None, None)), Settled::Has);
        assert_eq!(settle((T, None), None, &answer(None, None)), Settled::Has);
    }

    #[test]
    fn a_row_here_and_none_there_proves_nothing() {
        assert_eq!(
            settle((F, T), Some([9; 16]), &answer(None, Some(F))),
            Settled::Named
        );
    }

    #[test]
    fn any_other_answer_is_named() {
        assert_eq!(
            settle((F, T), Some([9; 16]), &answer(OTHER, Some(OTHER))),
            Settled::Named
        );
    }

    #[test]
    fn a_batch_splits_into_parts_that_each_fit_and_loses_nothing() {
        let all: Vec<Asked> = (0..200).map(asked).collect();
        let split = parts(all.clone());
        assert!(split.len() > 1);
        for part in &split {
            let bytes: usize = part.iter().map(|one| one.line.size()).sum();
            assert!(bytes <= super::PART, "{bytes}");
        }
        let joined: Vec<Asked> = split.into_iter().flatten().collect();
        assert_eq!(joined, all);
    }

    #[test]
    fn a_wazzup_with_nothing_to_ask_is_still_one_part() {
        assert_eq!(parts(Vec::new()).len(), 1);
    }

    #[test]
    fn a_part_counts_as_answered_once_and_the_rest_stay_unanswered() {
        let mut asking = Asking::new(1, (0..200).map(asked).collect(), None);
        let count = asking.parts.len() as u64;
        assert_eq!(asking.unanswered("rzr").len() as u64, count);
        assert!(asking.answered("rzr", 0));
        assert!(!asking.answered("rzr", 0));
        assert_eq!(asking.unanswered("rzr").len() as u64, count - 1);
        assert_eq!(asking.unanswered("other").len() as u64, count);
    }

    #[test]
    fn a_peer_is_named_only_once_it_has_answered_every_part() {
        let mut asking = Asking::new(1, vec![asked(1)], None);
        asking.name("rzr", &asked(1));
        assert_eq!(asking.take_named("rzr"), None);
        asking.answered("rzr", 0);
        let line = asking.take_named("rzr").unwrap_or_default();
        assert!(line.contains("rzr has not acknowledged 1 change"), "{line}");
        assert_eq!(asking.take_named("rzr"), None);
    }

    #[test]
    fn a_long_list_names_ten_keys_and_a_count() {
        let named: Vec<String> = (0..25).map(|n| format!("row {n}")).collect();
        let line = named_line("rzr", &named);
        assert!(line.contains("25 changes"), "{line}");
        assert!(
            line.contains("row 9") && !line.contains("row 10,"),
            "{line}"
        );
        assert!(line.contains("and 15 more"), "{line}");
    }

    #[test]
    fn an_asked_change_carries_what_settling_needs() {
        let envelope = Envelope {
            v: VERSION,
            node: "here".to_owned(),
            change: smugglr_core::change::Change {
                op: smugglr_core::change::op(F, T),
                table: "notes".to_owned(),
                key: vec!["'1'".to_owned()],
                from: F,
                to: T,
                values: Vec::new(),
            },
            targets: Vec::new(),
            sunset: 0,
            txn: None,
            key_id: "key".to_owned(),
            stamp: smugglr_core::shape::Stamp([0; 16]),
            signature: [7; 32],
        };
        let one = Asked::of(&envelope);
        assert_eq!((one.from, one.line.to), (F, T));
        assert_eq!(one.signature, [7; 32]);
    }
}
