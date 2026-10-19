//! Steps for `--sync-mode last`: the steering column the config names, read
//! from each place's rows by the same plan the CLI runs.

use rstest_bdd_macros::{given, then, when};
use serde_json::Value;
use smugglr_core::plan::SyncMode;
use smugglr_core::steering::Steering;

use crate::plan_steps::{
    Pair, Rows, Tables, change, entry, holds_key, key, run, watch, watched_key,
};

/// An orders row: its id, item and steering value.
fn order(id: &str, item: &str, steered: &str) -> Vec<String> {
    vec![
        format!("'{id}'"),
        format!("'{item}'"),
        format!("'{steered}'"),
    ]
}

/// Two places holding one order, synced once, whose orders table steers by
/// `column` as `steering` names it; then the order changed in both, its
/// steering value `in_source` in the source and `in_target` in the target.
fn order_changed_in_both(
    pair: &Pair,
    column: &str,
    steering: Steering,
    in_source: &str,
    in_target: &str,
) -> Result<(), String> {
    let columns = vec!["id".to_owned(), "item".to_owned(), column.to_owned()];
    pair.columns.set([("orders".to_owned(), columns)].into());
    let orders: Rows = [(key("o1"), order("o1", "tent", "2026-10-01T09:00:00Z"))].into();
    let tables: Tables = [("orders".to_owned(), orders)].into();
    pair.source.set(tables.clone());
    pair.target.set(tables);
    run(pair, SyncMode::Stop)?;
    pair.steering.set(steering);
    change(&pair.source, "orders", |rows| {
        rows.insert(key("o1"), order("o1", "source copy", in_source));
    })?;
    change(&pair.target, "orders", |rows| {
        rows.insert(key("o1"), order("o1", "target copy", in_target));
    })?;
    watch(pair, "orders", "o1");
    Ok(())
}

fn for_all(column: &str) -> Steering {
    Steering {
        all: Some(column.to_owned()),
        ..Steering::default()
    }
}

#[given(
    "a config naming updated_at as the steering column, and a row of orders changed in both places with a later updated_at in the target"
)]
fn later_in_target(pair: &Pair) -> Result<(), String> {
    order_changed_in_both(
        pair,
        "updated_at",
        for_all("updated_at"),
        "2026-10-01T10:00:00Z",
        "2026-10-01T11:00:00Z",
    )
}

#[given(
    "a config naming updated_at as the steering column, and a row of orders changed in both places with the same updated_at"
)]
fn same_in_both(pair: &Pair) -> Result<(), String> {
    order_changed_in_both(
        pair,
        "updated_at",
        for_all("updated_at"),
        "2026-10-01T10:00:00Z",
        "2026-10-01T10:00:00Z",
    )
}

#[given(
    "a config naming modified as the steering column for orders only, and a row of orders changed in both places with a later modified in the target"
)]
fn per_table_later_in_target(pair: &Pair) -> Result<(), String> {
    let steering = Steering {
        all: None,
        tables: [("orders".to_owned(), "modified".to_owned())].into(),
    };
    order_changed_in_both(
        pair,
        "modified",
        steering,
        "2026-10-01T10:00:00Z",
        "2026-10-01T11:00:00Z",
    )
}

#[when("the agent runs smugglr sync on them with --sync-mode last")]
fn agent_syncs_last(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Last)
}

#[when("the agent runs smugglr sync on them with --sync-mode last --json")]
fn agent_syncs_last_json(pair: &Pair) -> Result<(), String> {
    run(pair, SyncMode::Last)
}

#[then("the record gives orders the status stopped with that row's key in its rows")]
fn orders_stopped(pair: &Pair) -> Result<(), String> {
    if let Some(refused) = pair.refused.get() {
        return Err(format!("the run refused: {refused}"));
    }
    let orders = entry(pair, "orders")?;
    if orders.get("status").and_then(Value::as_str) != Some("stopped") {
        return Err(format!("orders is not stopped: {orders}"));
    }
    holds_key(&orders, "rows", &watched_key(pair)?)
}
