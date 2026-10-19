//! The Change and the one decision path (FR-SMUGGLR-075).
//!
//! Every sync type decides its changes as a [`Change`] through [`decide`]:
//! a direct sync turns each key's last hash per pair of places into Changes,
//! and multi and the mesh carry each Change in an envelope. `from` is the
//! row's hash before the change and `to` after it, both [`row_hash`]es; `None`
//! is no row.
//!
//! [`row_hash`]: crate::row_hash::row_hash

/// What a change does to its row, as the envelope's `op` names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// `up`.
    Up,
    /// `down`.
    Down,
    /// `upsert`.
    Upsert,
}

/// One change to one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    /// What the change does.
    pub op: Op,
    /// The table the row is in.
    pub table: String,
    /// SQLite's `quote()` text of each key column, in order.
    pub key: Vec<String>,
    /// The row's hash before the change; `None` when there was no row.
    pub from: Option<[u8; 16]>,
    /// The row's hash after the change; `None` when no row is left.
    pub to: Option<[u8; 16]>,
    /// SQLite's `quote()` text of each column after the change, in order.
    pub values: Vec<String>,
}

/// What a place does with a change that arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The row here is at the change's `from`: write the change's values.
    Apply,
    /// The row here is already at the change's `to`: leave it.
    Skip,
    /// The place has its own pending change of the row from the same
    /// `from` to a different `to`: the row changed in both places, and
    /// `--sync-mode` says what happens.
    Conflict,
    /// The row here matches neither: the change waits.
    Hold,
}

/// Decides a change against the hash of its row here (`None` when the place
/// holds no row for the key) and the place's own pending change of the same
/// row, if it has one.
#[must_use]
pub fn decide(change: &Change, here: Option<[u8; 16]>, pending: Option<&Change>) -> Decision {
    if change.to == here {
        Decision::Skip
    } else if pending.is_some_and(|own| own.from == change.from && own.to != change.to) {
        Decision::Conflict
    } else if change.from == here {
        Decision::Apply
    } else {
        Decision::Hold
    }
}

/// What a change from `from` to `to` does to its row.
#[must_use]
pub fn op(from: Option<[u8; 16]>, to: Option<[u8; 16]>) -> Op {
    match (from, to) {
        (_, None) => Op::Down,
        (None, Some(_)) => Op::Up,
        (Some(_), Some(_)) => Op::Upsert,
    }
}

#[cfg(test)]
mod tests {
    use super::{Change, Decision, Op, decide};

    #[test]
    fn the_same_from_with_a_different_to_is_a_conflict() {
        let own = change(Some([1; 16]), Some([3; 16]));
        assert_eq!(
            decide(
                &change(Some([1; 16]), Some([2; 16])),
                Some([3; 16]),
                Some(&own)
            ),
            Decision::Conflict
        );
    }

    fn change(from: Option<[u8; 16]>, to: Option<[u8; 16]>) -> Change {
        Change {
            op: Op::Upsert,
            table: "t".to_owned(),
            key: vec!["1".to_owned()],
            from,
            to,
            values: Vec::new(),
        }
    }

    #[test]
    fn a_change_matching_neither_hash_waits() {
        assert_eq!(
            decide(&change(Some([1; 16]), Some([2; 16])), Some([3; 16]), None),
            Decision::Hold
        );
    }
}
