//! What a first sync weighs before it moves anything, and the one limit on
//! what goes over the wire (FR-SMUGGLR-065).
//!
//! A first sync has no memory to tell it what changed, so it may have to
//! copy every row. It weighs itself first: the rows it would write, their
//! bytes and the tables they are in. Over `max_wire` it writes nothing and
//! says how to move the data instead. `max_wire` is also the limit of the
//! mesh's too-much answer (FR-SMUGGLR-079).

use std::fmt;

use serde::Serialize;

/// `max_wire` when the config does not set it: 100 MB.
///
/// Current choice (2026-10-19): MB is 10^6 bytes, so the default is
/// 100,000,000 bytes, because the record writes sizes in the same decimal
/// units (`2.1 GB`); revisable.
pub const DEFAULT_MAX_WIRE: u64 = 100_000_000;

/// What a sync would move.
///
/// Current choice (2026-10-19): a row's bytes are the length of SQLite's
/// `quote()` text of each of its columns, because that text is what travels
/// between places; revisable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Weight {
    /// Rows written, in either place.
    pub rows: u64,
    /// Their bytes.
    pub bytes: u64,
    /// Tables with at least one row written.
    pub tables: u64,
}

impl Weight {
    /// Whether this is more than `max_wire` bytes allows.
    #[must_use]
    pub fn over(&self, max_wire: u64) -> bool {
        self.bytes > max_wire
    }
}

/// `n` with a comma between each group of three digits.
fn grouped(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// `bytes` in decimal units with one decimal: `512 B`, `2.1 GB`.
fn size(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["kB", "MB", "GB", "TB", "PB", "EB"];
    if bytes < 1000 {
        return format!("{bytes} B");
    }
    let mut unit = 1000_u128;
    for (i, name) in UNITS.iter().enumerate() {
        let tenths = (u128::from(bytes) * 10 + unit / 2) / unit;
        if tenths < 10_000 || i == UNITS.len() - 1 {
            return format!("{}.{} {name}", tenths / 10, tenths % 10);
        }
        unit *= 1000;
    }
    format!("{bytes} B")
}

fn plural(n: u64, one: &str, many: &str) -> String {
    format!("{} {}", grouped(n), if n == 1 { one } else { many })
}

/// `4,812,000 rows, 2.1 GB in 3 tables`.
impl fmt::Display for Weight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, {} in {}",
            plural(self.rows, "row", "rows"),
            size(self.bytes),
            plural(self.tables, "table", "tables")
        )
    }
}

/// What a first sync over `max_wire` says: its weight, and how to move the
/// data from `source` to `target` instead.
#[must_use]
pub fn too_much(weight: &Weight, source: &str, target: &str) -> String {
    format!(
        "Too much for one sync: {weight}.\nMove it with: smugglr stash {source} <dir> \
         --key-file <path> --node <name> --whole, then smugglr retrieve {target} <dir> \
         --key-file <path> --node <name>"
    )
}

#[cfg(test)]
mod tests {
    use super::{Weight, too_much};

    #[test]
    fn the_weight_reads_as_the_spec_writes_it() {
        let weight = Weight {
            rows: 4_812_000,
            bytes: 2_100_000_000,
            tables: 3,
        };
        assert_eq!(
            too_much(&weight, "./shop.db", "d1://shop"),
            "Too much for one sync: 4,812,000 rows, 2.1 GB in 3 tables.\n\
             Move it with: smugglr stash ./shop.db <dir> --key-file <path> --node <name> --whole, \
             then smugglr retrieve d1://shop <dir> --key-file <path> --node <name>"
        );
    }

    #[test]
    fn small_weights_read_plainly() {
        let weight = Weight {
            rows: 1,
            bytes: 999,
            tables: 1,
        };
        assert_eq!(weight.to_string(), "1 row, 999 B in 1 table");
        let weight = Weight {
            rows: 0,
            bytes: 999_960,
            tables: 0,
        };
        assert_eq!(weight.to_string(), "0 rows, 1.0 MB in 0 tables");
    }
}
