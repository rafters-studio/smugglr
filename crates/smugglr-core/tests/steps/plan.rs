//! Steps for the plan and the record: two places simulated in memory, synced
//! by the same `plan` the CLI runs, each write applied with the other side's
//! copy, and the record made from the plans by the same `record::table`.

use std::collections::BTreeMap;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use serde_json::Value;
use smugglr_core::change::Op;
use smugglr_core::plan::{Hashes, Plan, Side, SyncMode, plan};
use smugglr_core::record::{self, Record};
use smugglr_core::row_hash::row_hash;

/// One table's rows by key, each row its columns' `quote()` text.
type Rows = BTreeMap<Vec<String>, Vec<String>>;

/// A place: its tables' rows by table name.
type Tables = BTreeMap<String, Rows>;

/// Two places, the pair's memory, the last record, and the row a scenario
/// watches (its table and key).
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Pair {
    source: Slot<Tables>,
    target: Slot<Tables>,
    memory: Slot<BTreeMap<String, Hashes>>,
    record: Slot<Value>,
    json: Slot<String>,
    watched: Slot<(String, Vec<String>)>,
}

#[fixture]
pub(crate) fn pair() -> Pair {
    Pair::default()
}

fn key(id: &str) -> Vec<String> {
    vec![format!("'{id}'")]
}

fn row(id: &str, body: &str) -> Vec<String> {
    vec![format!("'{id}'"), format!("'{body}'")]
}

fn hashes(rows: &Rows) -> Hashes {
    rows.iter()
        .map(|(key, row)| {
            let columns: Vec<&str> = row.iter().map(String::as_str).collect();
            (key.clone(), row_hash(&columns))
        })
        .collect()
}

/// One run: plan each table from both sides' hashes and memory, write, and
/// make the record.
fn run(pair: &Pair, mode: SyncMode) -> Result<(), String> {
    let mut source = pair.source.take().ok_or("no source")?;
    let mut target = pair.target.take().ok_or("no target")?;
    let mut memory = pair.memory.take().unwrap_or_default();
    let mut record = Record::default();
    let empty = Rows::new();
    let names: Vec<String> = source.keys().cloned().collect();
    for name in names {
        let last = memory.get(&name).cloned().unwrap_or_default();
        let planned = plan(
            &hashes(source.get(&name).unwrap_or(&empty)),
            &hashes(target.get(&name).unwrap_or(&empty)),
            &last,
            mode,
        );
        if let Plan::Move {
            writes,
            memory: next,
        } = &planned
        {
            for write in writes {
                let (from, to) = match write.side {
                    Side::Target => (&source, &mut target),
                    Side::Source => (&target, &mut source),
                };
                let copy = from
                    .get(&name)
                    .and_then(|rows| rows.get(&write.key))
                    .cloned();
                let rows = to.entry(name.clone()).or_default();
                match (write.op, copy) {
                    (Op::Down, _) => {
                        rows.remove(&write.key);
                    }
                    (_, Some(copy)) => {
                        rows.insert(write.key.clone(), copy);
                    }
                    (_, None) => return Err("no row to copy".to_owned()),
                }
            }
            memory.insert(name.clone(), next.clone());
        }
        record.tables.insert(name, record::table(&planned));
    }
    let json = record.json();
    pair.record
        .set(serde_json::from_str(&json).map_err(|err| format!("the record is not JSON: {err}"))?);
    pair.json.set(json);
    pair.memory.set(memory);
    pair.source.set(source);
    pair.target.set(target);
    Ok(())
}

/// Two places holding the same notes and orders, synced once.
fn synced(pair: &Pair) -> Result<(), String> {
    let notes: Rows = [("n1", "first"), ("n2", "second")]
        .into_iter()
        .map(|(id, body)| (key(id), row(id, body)))
        .collect();
    let orders: Rows = [(key("o1"), row("o1", "one"))].into_iter().collect();
    let tables: Tables = [("notes".to_owned(), notes), ("orders".to_owned(), orders)]
        .into_iter()
        .collect();
    pair.source.set(tables.clone());
    pair.target.set(tables);
    run(pair, SyncMode::Stop)
}

fn change(slot: &Slot<Tables>, table: &str, f: impl FnOnce(&mut Rows)) -> Result<(), String> {
    slot.with_mut(|tables| f(tables.entry(table.to_owned()).or_default()))
        .ok_or_else(|| "no place".to_owned())
}

fn watch(pair: &Pair, table: &str, id: &str) {
    pair.watched.set((table.to_owned(), key(id)));
}

fn edited_in_both(pair: &Pair) -> Result<(), String> {
    change(&pair.source, "notes", |rows| {
        rows.insert(key("n1"), row("n1", "edited in the source"));
    })?;
    change(&pair.target, "notes", |rows| {
        rows.insert(key("n1"), row("n1", "edited in the target"));
    })?;
    watch(pair, "notes", "n1");
    Ok(())
}

#[given("two places synced before, and a row since added to the source")]
fn added_to_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, "notes", |rows| {
        rows.insert(key("n3"), row("n3", "third"));
    })?;
    watch(pair, "notes", "n3");
    Ok(())
}

#[given("two places synced before, and a row since deleted in the source")]
fn deleted_in_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, "notes", |rows| {
        rows.remove(&key("n1"));
    })?;
    watch(pair, "notes", "n1");
    Ok(())
}

#[given("two places synced before, and a row since edited in the target only")]
fn edited_in_target(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.target, "notes", |rows| {
        rows.insert(key("n1"), row("n1", "edited"));
    })?;
    watch(pair, "notes", "n1");
    Ok(())
}

#[given("two places synced before, and a row of notes since edited in both")]
fn notes_edited_in_both(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    edited_in_both(pair)
}

#[given(
    "two places synced before, and a row of notes since edited in the source and deleted in the target"
)]
fn edited_against_deleted(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, "notes", |rows| {
        rows.insert(key("n1"), row("n1", "edited in the source"));
    })?;
    change(&pair.target, "notes", |rows| {
        rows.remove(&key("n1"));
    })?;
    watch(pair, "notes", "n1");
    Ok(())
}

#[given("two places where notes has a row changed in both and orders has rows to move")]
fn notes_stop_orders_move(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    edited_in_both(pair)?;
    change(&pair.source, "orders", |rows| {
        rows.insert(key("o2"), row("o2", "two"));
    })
}

#[when("the agent runs smugglr sync on them")]
fn agent_syncs(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Stop)
}

#[when("the agent runs smugglr sync on them with --json")]
fn agent_syncs_json(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Stop)
}

#[when("the agent runs smugglr sync on them with --sync-mode source")]
fn agent_syncs_source(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Source)
}

#[when("the agent runs smugglr sync on them with --sync-mode source --json")]
fn agent_syncs_source_json(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Source)
}

#[when("the agent runs smugglr sync on them with --sync-mode target")]
fn agent_syncs_target(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Target)
}

#[when("the agent runs smugglr sync on them with --sync-mode target --json")]
fn agent_syncs_target_json(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Target)
}

/// The watched row in the source and in the target.
type Copies = (Option<Vec<String>>, Option<Vec<String>>);

fn watched_copies(pair: &Pair) -> Result<Copies, String> {
    let (table, key) = pair.watched.get().ok_or("no row watched")?;
    let source = pair.source.get().ok_or("no source")?;
    let target = pair.target.get().ok_or("no target")?;
    let get = |tables: &Tables| tables.get(&table).and_then(|rows| rows.get(&key)).cloned();
    Ok((get(&source), get(&target)))
}

#[then("the target holds that row")]
fn target_holds(pair: &Pair) -> Result<(), String> {
    match watched_copies(pair)? {
        (Some(want), Some(got)) if want == got => Ok(()),
        (want, got) => Err(format!("the source holds {want:?}, the target {got:?}")),
    }
}

#[then("the row is gone from the target")]
fn gone_from_target(pair: &Pair) -> Result<(), String> {
    match watched_copies(pair)? {
        (_, None) => Ok(()),
        (_, Some(row)) => Err(format!("the target still holds {row:?}")),
    }
}

#[then("the source's row holds the target's values")]
fn source_holds_target(pair: &Pair) -> Result<(), String> {
    match watched_copies(pair)? {
        (Some(got), Some(want)) if want == got && got.iter().all(|c| !c.contains("source")) => {
            Ok(())
        }
        (got, want) => Err(format!("the source holds {got:?}, the target {want:?}")),
    }
}

#[then("the target's row holds the source's values")]
fn target_holds_source(pair: &Pair) -> Result<(), String> {
    match watched_copies(pair)? {
        (Some(want), Some(got)) if want == got && got.iter().all(|c| !c.contains("target")) => {
            Ok(())
        }
        (want, got) => Err(format!("the source holds {want:?}, the target {got:?}")),
    }
}

/// The record's entry for `table`.
fn entry(pair: &Pair, table: &str) -> Result<Value, String> {
    let record = pair.record.get().ok_or("no record")?;
    record
        .pointer(&format!("/tables/{table}"))
        .cloned()
        .ok_or_else(|| format!("the record has no entry for {table}: {record}"))
}

/// The watched row's key as the record writes it.
fn watched_key(pair: &Pair) -> Result<Value, String> {
    let (_, key) = pair.watched.get().ok_or("no row watched")?;
    match key.as_slice() {
        [column] => Ok(Value::String(column.trim_matches('\'').to_owned())),
        _ => Err(format!("a key of {} columns", key.len())),
    }
}

fn holds_key(entry: &Value, field: &str, key: &Value) -> Result<(), String> {
    match entry.get(field).and_then(Value::as_array) {
        Some(keys) if keys.contains(key) => Ok(()),
        _ => Err(format!("{field} does not hold {key}: {entry}")),
    }
}

#[then("the record gives notes the status stopped with that row's key in its rows")]
fn notes_stopped(pair: &Pair) -> Result<(), String> {
    let notes = entry(pair, "notes")?;
    if notes.get("status").and_then(Value::as_str) != Some("stopped") {
        return Err(format!("notes is not stopped: {notes}"));
    }
    holds_key(&notes, "rows", &watched_key(pair)?)
}

#[then("the why for notes says the rows changed in both places and names --sync-mode")]
fn why_for_notes(pair: &Pair) -> Result<(), String> {
    let notes = entry(pair, "notes")?;
    match notes.get("why").and_then(Value::as_str) {
        Some(why) if why.contains("changed in both places") && why.contains("--sync-mode") => {
            Ok(())
        }
        _ => Err(format!("notes gives no such why: {notes}")),
    }
}

#[then("the record gives orders the status moved")]
fn orders_moved(pair: &Pair) -> Result<(), String> {
    let orders = entry(pair, "orders")?;
    match orders.get("status").and_then(Value::as_str) {
        Some("moved") => Ok(()),
        _ => Err(format!("orders did not move: {orders}")),
    }
}

#[then("the record's overwritten_in_target for notes holds that row's key")]
fn overwritten_in_target(pair: &Pair) -> Result<(), String> {
    holds_key(
        &entry(pair, "notes")?,
        "overwritten_in_target",
        &watched_key(pair)?,
    )
}

#[then("the record's overwritten_in_source for notes holds that row's key")]
fn overwritten_in_source(pair: &Pair) -> Result<(), String> {
    holds_key(
        &entry(pair, "notes")?,
        "overwritten_in_source",
        &watched_key(pair)?,
    )
}

#[then("the record does not contain the word wins")]
fn no_wins(pair: &Pair) -> Result<(), String> {
    let json = pair.json.get().ok_or("no record")?;
    let found = json
        .split(|c: char| !c.is_alphanumeric())
        .any(|word| word.eq_ignore_ascii_case("wins"));
    if found {
        Err(format!("the record says wins: {json}"))
    } else {
        Ok(())
    }
}
