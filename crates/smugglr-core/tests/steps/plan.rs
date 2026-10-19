//! Steps for the plan: two places simulated in memory, synced by the same
//! `plan` the CLI runs, applying each write with the other side's copy.

use std::collections::BTreeMap;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use smugglr_core::change::Op;
use smugglr_core::plan::{Hashes, Plan, Side, plan};
use smugglr_core::row_hash::row_hash;

/// One table's rows by key, each row its columns' `quote()` text.
pub(crate) type Rows = BTreeMap<Vec<String>, Vec<String>>;

/// Two places, the pair's memory, and the row a scenario watches.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Pair {
    source: Slot<Rows>,
    target: Slot<Rows>,
    memory: Slot<Hashes>,
    watched: Slot<Vec<String>>,
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

/// One run: plan from both sides' hashes and memory, then write.
pub(crate) fn run(pair: &Pair) -> Result<Plan, String> {
    let mut source = pair.source.take().ok_or("no source")?;
    let mut target = pair.target.take().ok_or("no target")?;
    let memory = pair.memory.take().unwrap_or_default();
    let planned = plan(&hashes(&source), &hashes(&target), &memory);
    match &planned {
        Plan::Move { writes, memory } => {
            for write in writes {
                let (from, to) = match write.side {
                    Side::Target => (&source, &mut target),
                    Side::Source => (&target, &mut source),
                };
                if write.op == Op::Down {
                    to.remove(&write.key);
                } else {
                    let copy = from.get(&write.key).ok_or("no row to copy")?.clone();
                    to.insert(write.key.clone(), copy);
                }
            }
            pair.memory.set(memory.clone());
        }
        Plan::Stop { .. } => pair.memory.set(memory),
    }
    pair.source.set(source);
    pair.target.set(target);
    Ok(planned)
}

/// Two places holding the same two notes, synced once.
fn synced(pair: &Pair) -> Result<(), String> {
    let rows: Rows = [("n1", "first"), ("n2", "second")]
        .into_iter()
        .map(|(id, body)| (key(id), row(id, body)))
        .collect();
    pair.source.set(rows.clone());
    pair.target.set(rows);
    run(pair).map(|_| ())
}

fn change(slot: &Slot<Rows>, f: impl FnOnce(&mut Rows)) -> Result<(), String> {
    slot.with_mut(f).ok_or_else(|| "no place".to_owned())
}

#[given("two places synced before, and a row since added to the source")]
fn added_to_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, |rows| {
        rows.insert(key("n3"), row("n3", "third"));
    })?;
    pair.watched.set(key("n3"));
    Ok(())
}

#[given("two places synced before, and a row since deleted in the source")]
fn deleted_in_source(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.source, |rows| {
        rows.remove(&key("n1"));
    })?;
    pair.watched.set(key("n1"));
    Ok(())
}

#[given("two places synced before, and a row since edited in the target only")]
fn edited_in_target(pair: &Pair) -> Result<(), String> {
    synced(pair)?;
    change(&pair.target, |rows| {
        rows.insert(key("n1"), row("n1", "edited"));
    })?;
    pair.watched.set(key("n1"));
    Ok(())
}

#[when("the agent runs smugglr sync on them")]
fn agent_syncs(pair: &Pair) -> Result<(), String> {
    run(pair).map(|_| ())
}

#[then("the target holds that row")]
fn target_holds(pair: &Pair) -> Result<(), String> {
    let watched = pair.watched.get().ok_or("no row watched")?;
    let source = pair.source.get().ok_or("no source")?;
    let target = pair.target.get().ok_or("no target")?;
    match (source.get(&watched), target.get(&watched)) {
        (Some(want), Some(got)) if want == got => Ok(()),
        (want, got) => Err(format!("the source holds {want:?}, the target {got:?}")),
    }
}

#[then("the row is gone from the target")]
fn gone_from_target(pair: &Pair) -> Result<(), String> {
    let watched = pair.watched.get().ok_or("no row watched")?;
    let target = pair.target.get().ok_or("no target")?;
    match target.get(&watched) {
        None => Ok(()),
        Some(row) => Err(format!("the target still holds {row:?}")),
    }
}

#[then("the source's row holds the target's values")]
fn source_holds_target(pair: &Pair) -> Result<(), String> {
    let watched = pair.watched.get().ok_or("no row watched")?;
    let source = pair.source.get().ok_or("no source")?;
    let target = pair.target.get().ok_or("no target")?;
    match (source.get(&watched), target.get(&watched)) {
        (Some(got), Some(want)) if want == got => Ok(()),
        (got, want) => Err(format!("the source holds {got:?}, the target {want:?}")),
    }
}
