//! Ignored tables (FR-SMUGGLR-066): smugglr never reads, writes, hashes or
//! sends them.
//!
//! Always ignored: SQLite's own `sqlite_*` tables. The person lists more,
//! with simple wildcards: `*` stands for any run of characters, `?` for any
//! one. Names match as SQLite
//! matches them, without regard to ASCII case.

use std::collections::BTreeSet;

/// The tables smugglr always ignores.
pub const ALWAYS: &[&str] = &["sqlite_*"];

/// Which tables a run ignores: those smugglr always ignores, and those the
/// person lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ignore {
    patterns: Vec<String>,
}

impl Default for Ignore {
    fn default() -> Self {
        Self::new(std::iter::empty::<String>())
    }
}

impl Ignore {
    /// The tables smugglr always ignores, and those `listed` names, each a
    /// table name or a pattern with `*` and `?`.
    #[must_use]
    pub fn new<I, S>(listed: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let patterns = ALWAYS
            .iter()
            .map(|&pattern| pattern.to_owned())
            .chain(listed.into_iter().map(Into::into))
            .collect();
        Self { patterns }
    }

    /// Whether the run ignores the table `name`.
    #[must_use]
    pub fn ignores(&self, name: &str) -> bool {
        self.patterns
            .iter()
            .any(|pattern| matches(pattern.as_bytes(), name.as_bytes()))
    }

    /// Splits `tables` into those the run keeps, in their order, and the
    /// names of the ignored ones, each once; `name` gives a table's name.
    #[must_use]
    pub fn split<T>(
        &self,
        tables: Vec<T>,
        name: impl Fn(&T) -> &str,
    ) -> (Vec<T>, BTreeSet<String>) {
        let mut ignored = BTreeSet::new();
        let kept = tables
            .into_iter()
            .filter(|table| {
                let name = name(table);
                if self.ignores(name) {
                    ignored.insert(name.to_owned());
                    false
                } else {
                    true
                }
            })
            .collect();
        (kept, ignored)
    }
}

/// Whether `name` matches `pattern`, `*` standing for any run of bytes and
/// `?` for any one, ASCII case aside.
fn matches(pattern: &[u8], name: &[u8]) -> bool {
    let (mut p, mut n) = (0, 0);
    // Where the last `*` was, and the name position it stood for up to then.
    let mut star: Option<(usize, usize)> = None;
    while n < name.len() {
        match pattern.get(p) {
            Some(b'*') => {
                star = Some((p, n));
                p += 1;
            }
            Some(&c) if c == b'?' || c.eq_ignore_ascii_case(&name[n]) => {
                p += 1;
                n += 1;
            }
            _ => match star {
                Some((at, from)) => {
                    p = at + 1;
                    n = from + 1;
                    star = Some((at, from + 1));
                }
                None => return false,
            },
        }
    }
    pattern[p..].iter().all(|&c| c == b'*')
}

#[cfg(test)]
mod tests {
    use super::Ignore;

    #[test]
    fn sqlite_tables_are_always_ignored() {
        let ignore = Ignore::default();
        assert!(ignore.ignores("sqlite_sequence"));
        assert!(ignore.ignores("sqlite_stat1"));
        assert!(!ignore.ignores("notes"));
        assert!(!ignore.ignores("app_sqlite_cache"));
    }

    #[test]
    fn wildcards_match_runs_and_single_characters() {
        let ignore = Ignore::new(["cache_*", "tmp?", "*_log"]);
        assert!(ignore.ignores("cache_pages"));
        assert!(ignore.ignores("cache_"));
        assert!(ignore.ignores("Cache_Thumbs"));
        assert!(ignore.ignores("tmp1"));
        assert!(!ignore.ignores("tmp12"));
        assert!(ignore.ignores("audit_log"));
        assert!(ignore.ignores("a_b_log"));
        assert!(!ignore.ignores("caches"));
        assert!(!ignore.ignores("audit_logs"));
    }

    #[test]
    fn a_plain_name_matches_only_itself() {
        let ignore = Ignore::new(["sessions_tmp"]);
        assert!(ignore.ignores("sessions_tmp"));
        assert!(!ignore.ignores("sessions_tmp2"));
        assert!(!ignore.ignores("sessions"));
    }

    #[test]
    fn split_keeps_order_and_names_each_ignored_table_once() {
        let ignore = Ignore::new(["cache_*"]);
        let (kept, ignored) = ignore.split(
            vec!["notes", "cache_pages", "orders", "cache_pages"],
            |name| name,
        );
        assert_eq!(kept, vec!["notes", "orders"]);
        assert_eq!(ignored.into_iter().collect::<Vec<_>>(), vec!["cache_pages"]);
    }
}
