//! Steps for ignored tables: the simulated places of the plan steps, with
//! the config's ignore list, synced by the same run.

use rstest_bdd_macros::{given, then};
use serde_json::Value;

use crate::plan_steps::{Pair, Rows, Tables, key, row};

/// One table's rows, from `(id, body)` pairs.
fn rows(rows: &[(&str, &str)]) -> Rows {
    rows.iter()
        .map(|&(id, body)| (key(id), row(id, body)))
        .collect()
}

/// Sets both places: each holds notes, the same in both, and `source` and
/// `target`'s tables beside it.
fn places(pair: &Pair, source: Vec<(&str, Rows)>, target: Vec<(&str, Rows)>) {
    let notes = rows(&[("n1", "first")]);
    let tables = |extra: Vec<(&str, Rows)>| -> Tables {
        std::iter::once(("notes".to_owned(), notes.clone()))
            .chain(
                extra
                    .into_iter()
                    .map(|(name, rows)| (name.to_owned(), rows)),
            )
            .collect()
    };
    pair.source.set(tables(source));
    pair.target.set(tables(target));
}

fn ignoring(pair: &Pair, listed: &[&str]) {
    pair.ignore
        .set(listed.iter().map(|&name| name.to_owned()).collect());
}

#[given(
    "a config whose ignore list holds sessions_tmp, and two places whose sessions_tmp tables differ"
)]
fn sessions_tmp_differ(pair: &Pair) {
    ignoring(pair, &["sessions_tmp"]);
    places(
        pair,
        vec![("sessions_tmp", rows(&[("s1", "source"), ("s2", "source")]))],
        // Rows the target lacks, which a sync of the table would move.
        vec![("sessions_tmp", rows(&[("s1", "source")]))],
    );
}

#[given(
    "a config whose ignore list holds cache_*, and two places whose cache_pages and cache_thumbs tables differ"
)]
fn caches_differ(pair: &Pair) {
    ignoring(pair, &["cache_*"]);
    places(
        pair,
        vec![
            ("cache_pages", rows(&[("p1", "source")])),
            ("cache_thumbs", rows(&[("t1", "source")])),
        ],
        vec![
            ("cache_pages", rows(&[("p1", "target")])),
            ("cache_thumbs", rows(&[])),
        ],
    );
}

#[given("two places whose sqlite_sequence tables differ")]
fn sqlite_sequence_differs(pair: &Pair) {
    places(
        pair,
        vec![("sqlite_sequence", rows(&[("notes", "7"), ("orders", "2")]))],
        vec![("sqlite_sequence", rows(&[("notes", "7")]))],
    );
}

#[given(
    "a config whose ignore list holds cache_* and sessions_tmp, and two places holding cache_pages, cache_thumbs and sessions_tmp"
)]
fn caches_and_sessions(pair: &Pair) {
    ignoring(pair, &["cache_*", "sessions_tmp"]);
    let tables = || {
        vec![
            ("cache_pages", rows(&[("p1", "page")])),
            ("cache_thumbs", rows(&[("t1", "thumb")])),
            ("sessions_tmp", rows(&[("s1", "session")])),
        ]
    };
    places(pair, tables(), tables());
}

/// Fails unless the target's `table` holds what it held before the run.
fn target_unchanged(pair: &Pair, table: &str) -> Result<(), String> {
    let (_, before) = pair.before.get().ok_or("smugglr was not run")?;
    let after = pair.target.get().ok_or("no target")?;
    if before.get(table) == after.get(table) {
        Ok(())
    } else {
        Err(format!(
            "the target's {table} was {:?} and is now {:?}",
            before.get(table),
            after.get(table)
        ))
    }
}

#[then("the target's sessions_tmp is unchanged")]
fn sessions_tmp_unchanged(pair: &Pair) -> Result<(), String> {
    target_unchanged(pair, "sessions_tmp")
}

#[then("the target's sqlite_sequence is unchanged")]
fn sqlite_sequence_unchanged(pair: &Pair) -> Result<(), String> {
    target_unchanged(pair, "sqlite_sequence")
}

/// The record's ignored list, and the record.
fn ignored(pair: &Pair) -> Result<(Vec<String>, Value), String> {
    let record = pair.record.get().ok_or("smugglr was not run")?;
    let list = record
        .get("ignored")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("the record has no ignored list: {record}"))?
        .iter()
        .map(|name| {
            name.as_str()
                .map(str::to_owned)
                .ok_or_else(|| format!("{name} in the ignored list is not a name"))
        })
        .collect::<Result<_, _>>()?;
    Ok((list, record))
}

#[then("the record's ignored list holds cache_pages and cache_thumbs")]
fn ignored_holds_caches(pair: &Pair) -> Result<(), String> {
    let (list, _) = ignored(pair)?;
    if ["cache_pages", "cache_thumbs"]
        .iter()
        .all(|name| list.iter().any(|held| held == name))
    {
        Ok(())
    } else {
        Err(format!("the record's ignored list is {list:?}"))
    }
}

#[then("the record's ignored list names cache_pages, cache_thumbs and sessions_tmp once each")]
fn ignored_once_each(pair: &Pair) -> Result<(), String> {
    let (list, record) = ignored(pair)?;
    for name in ["cache_pages", "cache_thumbs", "sessions_tmp"] {
        let times = list.iter().filter(|held| *held == name).count();
        if times != 1 {
            return Err(format!(
                "the ignored list names {name} {times} times: {list:?}"
            ));
        }
        if record.get("tables").and_then(|t| t.get(name)).is_some() {
            return Err(format!(
                "the record also names {name} among its tables: {record}"
            ));
        }
    }
    Ok(())
}
