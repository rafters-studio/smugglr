//! Steps for the plan and the record: two places simulated in memory, synced
//! by the same `plan` the CLI runs, each write applied with the other side's
//! copy, and the record made from the plans by the same `record::table`.

use std::collections::BTreeMap;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use serde_json::Value;
use smugglr_core::change::Op;
use smugglr_core::plan::{Hashes, Plan, Side, SyncMode, not_read_back, plan};
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
    /// The target keeps each written row with its text upper-cased.
    alters: Slot<bool>,
    /// Both places as they were before the last run.
    before: Slot<(Tables, Tables)>,
    /// Ids of rows a scenario added.
    added: Slot<Vec<String>>,
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
    run_as(pair, mode, false)
}

/// One run, or with `dryrun` what it would do, writing nothing.
fn run_as(pair: &Pair, mode: SyncMode, dryrun: bool) -> Result<(), String> {
    let mut source = pair.source.take().ok_or("no source")?;
    let mut target = pair.target.take().ok_or("no target")?;
    let mut memory = pair.memory.take().unwrap_or_default();
    let alters = pair.alters.get().unwrap_or(false);
    pair.before.set((source.clone(), target.clone()));
    let mut tables = BTreeMap::new();
    let empty = Rows::new();
    let names: Vec<String> = source.keys().cloned().collect();
    for name in names {
        let last = memory.get(&name).cloned().unwrap_or_default();
        let planned = plan(
            &name,
            &hashes(source.get(&name).unwrap_or(&empty)),
            &hashes(target.get(&name).unwrap_or(&empty)),
            &last,
            mode,
        );
        if let (
            Plan::Move {
                writes,
                memory: next,
            },
            false,
        ) = (&planned, dryrun)
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
                    (_, Some(mut copy)) => {
                        if alters && write.side == Side::Target {
                            copy = copy.iter().map(|c| c.to_uppercase()).collect();
                        }
                        rows.insert(write.key.clone(), copy);
                    }
                    (_, None) => return Err("no row to copy".to_owned()),
                }
            }
            // Read back what was written, each side's rows as it holds them.
            let mut mismatched = Vec::new();
            for (side, place) in [(Side::Target, &target), (Side::Source, &source)] {
                let written: Vec<_> = writes.iter().filter(|w| w.side == side).cloned().collect();
                let held = hashes(place.get(&name).unwrap_or(&empty));
                mismatched.extend(not_read_back(&written, &held));
            }
            if !mismatched.is_empty() {
                tables.insert(name, record::not_read_back(mismatched));
                continue;
            }
            memory.insert(name.clone(), next.clone());
        }
        tables.insert(name, record::table(&planned, dryrun));
    }
    let json = Record::new(tables, dryrun).json();
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

#[given("a target that keeps a written row differently from how it was written")]
fn altering_target(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    pair.alters.set(true);
    change(&pair.source, "notes", |rows| {
        rows.insert(key("n3"), row("n3", "third"));
    })?;
    watch(pair, "notes", "n3");
    Ok(())
}

#[when("the agent runs smugglr sync --json on the source and that target")]
fn agent_syncs_json_on_target(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Stop)
}

#[then("the record names that row's table and key")]
fn names_table_and_key(pair: &Pair) -> Result<(), String> {
    let (table, _) = pair.watched.get().ok_or("no row watched")?;
    holds_key(&entry(pair, &table)?, "not_read_back", &watched_key(pair)?)
}

/// Adds `count` new orders to the source, remembering their ids.
fn new_orders(pair: &Pair, count: usize) -> Result<(), String> {
    let ids: Vec<String> = (1..=count).map(|i| format!("new-order-{i:02}")).collect();
    change(&pair.source, "orders", |rows| {
        for id in &ids {
            rows.insert(key(id), row(id, "new"));
        }
    })?;
    pair.added.set(ids);
    Ok(())
}

#[given("two places where the source has rows the target lacks")]
fn source_has_rows(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    new_orders(pair, 3)
}

#[given("two places where the source's orders table has rows the target lacks")]
fn source_orders_has_rows(pair: &Pair) -> Result<(), String> {
    source_has_rows(pair)
}

#[given("two places where the source has rows the target lacks and nothing else differs")]
fn only_new_rows(pair: &Pair) -> Result<(), String> {
    source_has_rows(pair)
}

#[given("two places where only new rows move in orders")]
fn only_new_orders(pair: &Pair) -> Result<(), String> {
    source_has_rows(pair)
}

#[given("two places where the source's orders table has 41 rows the target lacks")]
fn forty_one_orders(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    new_orders(pair, 41)
}

#[given("two places whose notes table holds the same rows in both")]
fn notes_in_step(pair: &Pair) -> Result<(), String> {
    synced(pair)
}

#[given(
    "two places where orders has rows to move and notes has a row changed in both places since the last sync"
)]
fn orders_move_notes_stop(pair: &Pair) -> Result<(), String> {
    notes_stop_orders_move(pair)
}

#[given("two places whose only differences are rows changed in both places since the last sync")]
fn only_both_sides(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    edited_in_both(pair)
}

#[given("two places synced before, and a row of orders since edited in the source only")]
fn order_edited_in_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, "orders", |rows| {
        rows.insert(key("o1"), row("o1", "edited"));
    })?;
    watch(pair, "orders", "o1");
    Ok(())
}

#[given("two places synced before, and a row of orders since deleted in the source")]
fn order_deleted_in_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, "orders", |rows| {
        rows.remove(&key("o1"));
    })?;
    watch(pair, "orders", "o1");
    Ok(())
}

#[when("the agent runs smugglr sync on them with --dryrun")]
fn agent_dry_runs(pair: &Pair) -> Result<(), String> {
    run_as(pair, SyncMode::Stop, true)
}

#[when("the agent runs smugglr sync on them with --dryrun --json")]
fn agent_dry_runs_json(pair: &Pair) -> Result<(), String> {
    run_as(pair, SyncMode::Stop, true)
}

#[then("neither place has changed")]
fn neither_changed(pair: &Pair) -> Result<(), String> {
    let (source, target) = pair.before.get().ok_or("no run")?;
    let now = (
        pair.source.get().ok_or("no source")?,
        pair.target.get().ok_or("no target")?,
    );
    if now == (source, target) {
        Ok(())
    } else {
        Err(format!("a place changed: {now:?}"))
    }
}

fn status_of(pair: &Pair, table: &str, want: &str) -> Result<(), String> {
    let entry = entry(pair, table)?;
    match entry.get("status").and_then(Value::as_str) {
        Some(status) if status == want => Ok(()),
        _ => Err(format!("{table} is not {want}: {entry}")),
    }
}

#[then("the record gives orders the status would move")]
fn orders_would_move(pair: &Pair) -> Result<(), String> {
    status_of(pair, "orders", "would move")
}

#[then("the record gives notes the status in step")]
fn notes_in_step_status(pair: &Pair) -> Result<(), String> {
    status_of(pair, "notes", "in step")
}

fn run_status(pair: &Pair, want: &str) -> Result<(), String> {
    let record = pair.record.get().ok_or("no record")?;
    match record.get("status").and_then(Value::as_str) {
        Some(status) if status == want => Ok(()),
        _ => Err(format!("the run is not {want}: {record}")),
    }
}

#[then("the record's run status is preview")]
fn run_preview(pair: &Pair) -> Result<(), String> {
    run_status(pair, "preview")
}

#[then("the record's run status is done")]
fn run_done(pair: &Pair) -> Result<(), String> {
    run_status(pair, "done")
}

#[then("the record's run status is stopped in part")]
fn run_stopped_in_part(pair: &Pair) -> Result<(), String> {
    run_status(pair, "stopped in part")
}

#[then("the record's run status is stopped")]
fn run_stopped(pair: &Pair) -> Result<(), String> {
    run_status(pair, "stopped")
}

#[then(
    "the record's entry for orders carries to_target, to_source, deleted_in_target, deleted_in_source, overwritten_in_target and overwritten_in_source"
)]
fn six_fields(pair: &Pair) -> Result<(), String> {
    let orders = entry(pair, "orders")?;
    let fields = [
        "to_target",
        "to_source",
        "deleted_in_target",
        "deleted_in_source",
        "overwritten_in_target",
        "overwritten_in_source",
    ];
    match fields.iter().find(|field| orders.get(**field).is_none()) {
        None => Ok(()),
        Some(field) => Err(format!("orders carries no {field}: {orders}")),
    }
}

#[then("the record lists no key of those 41 rows")]
fn no_new_keys(pair: &Pair) -> Result<(), String> {
    let json = pair.json.get().ok_or("no record")?;
    let added = pair.added.get().ok_or("no rows added")?;
    let orders = entry(pair, "orders")?;
    if orders.get("to_target").and_then(Value::as_u64) != Some(41) {
        return Err(format!("orders did not count 41 rows: {orders}"));
    }
    match added.iter().find(|id| json.contains(id.as_str())) {
        None => Ok(()),
        Some(id) => Err(format!("the record lists {id}: {json}")),
    }
}

#[then("the record's overwritten_in_target for orders is empty")]
fn orders_overwritten_empty(pair: &Pair) -> Result<(), String> {
    let orders = entry(pair, "orders")?;
    match orders
        .get("overwritten_in_target")
        .and_then(Value::as_array)
    {
        Some(keys)
            if keys.is_empty() && orders.get("to_target").and_then(Value::as_u64) == Some(1) =>
        {
            Ok(())
        }
        _ => Err(format!("orders: {orders}")),
    }
}

#[then("the record's deleted_in_target for orders holds that row's key")]
fn orders_deleted_in_target(pair: &Pair) -> Result<(), String> {
    holds_key(
        &entry(pair, "orders")?,
        "deleted_in_target",
        &watched_key(pair)?,
    )
}
