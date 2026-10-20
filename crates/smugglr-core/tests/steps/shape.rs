//! Steps for a shape known only by its stamp: a device's table and the stamp
//! a blob carries for it, compared by the same `shape::compare_stamp` and put
//! in a record by the same `Record::shape_mismatch` any retrieve uses. The
//! retrieve command itself and the blob format are #519's and #523's; this
//! is the engine's part of it.

use std::fmt::Write as _;

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use serde_json::Value;
use smugglr_core::record::Record;
use smugglr_core::shape::{Column, Stamp, TableShape, column_hash, compare_stamp};

/// A device's notes table, the stamp a blob carries for it, and the record
/// of retrieving the blob.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Device {
    notes: Slot<TableShape>,
    blob: Slot<Stamp>,
    record: Slot<Value>,
}

#[fixture]
pub(crate) fn device() -> Device {
    Device::default()
}

fn columns(columns: &[(&str, &str)]) -> Vec<Column> {
    columns
        .iter()
        .map(|&(name, kind)| Column {
            name: name.to_owned(),
            kind: kind.to_owned(),
        })
        .collect()
}

fn hex(hash: [u8; 16]) -> String {
    hash.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

#[given("a device and a blob whose shape stamp for notes differs from the device's")]
fn device_and_blob(device: &Device) {
    device.notes.set(TableShape {
        name: "notes".to_owned(),
        columns: columns(&[("id", "TEXT"), ("body", "TEXT"), ("created_at", "INTEGER")]),
    });
    // The blob was stashed from a place whose notes has no created_at.
    device.blob.set(Stamp(column_hash(&columns(&[
        ("id", "TEXT"),
        ("body", "TEXT"),
    ]))));
}

#[when("the agent runs smugglr retrieve of that blob on the device with --json")]
fn retrieve(device: &Device) -> Result<(), String> {
    let notes = device.notes.get().ok_or("no device")?;
    let blob = device.blob.get().ok_or("no blob")?;
    let mismatch = compare_stamp(&notes, blob).ok_or("the stamps match")?;
    let json = Record::shape_mismatch(mismatch).json();
    device
        .record
        .set(serde_json::from_str(&json).map_err(|err| format!("{err}: {json}"))?);
    Ok(())
}

#[then("the record names notes, the device's columns of notes and both stamps")]
fn names_what_it_can(device: &Device) -> Result<(), String> {
    let record = device.record.get().ok_or("nothing was retrieved")?;
    let notes = device.notes.get().ok_or("no device")?;
    let Some(Stamp(blob)) = device.blob.get() else {
        return Err("no blob".to_owned());
    };
    let shape = record.get("shape").ok_or("the record names no shape")?;
    let named: Vec<&str> = shape
        .get("columns")
        .and_then(Value::as_array)
        .map(|columns| {
            columns
                .iter()
                .filter_map(|c| c.get("name").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    let want: Vec<&str> = notes.columns.iter().map(|c| c.name.as_str()).collect();
    let stamp = |field: &str| shape.get(field).and_then(Value::as_str).map(str::to_owned);
    if shape.get("table").and_then(Value::as_str) == Some("notes")
        && named == want
        && stamp("stamp") == Some(hex(column_hash(&notes.columns)))
        && stamp("arrived") == Some(hex(blob))
    {
        Ok(())
    } else {
        Err(format!("the record is {record}"))
    }
}
