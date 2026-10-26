//! Steps for multi: devices' databases kept in step by smugglr stash and
//! smugglr retrieve, a directory in the scenario's folder standing for the
//! bucket.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use serde_json::Value as Json;
use smugglr_core::envelope::GRAVEYARD;
use smugglr_core::plan::SyncMode;
use smugglr_core::record::{Record, Status};
use smugglr_multi::{Blob, Settings};
use smugglr_place::LocalFile;

use crate::cli::config;
use crate::http_sql::service;
use crate::setup::{NOTES, ORDERS, THREE_NOTES, World, rows_of, sql_on};

/// The devices of a scenario and the blobs they stashed.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Devices {
    /// Each device's address, by name.
    places: Slot<BTreeMap<String, String>>,
    /// The blobs stashed, in the order they were stashed.
    blobs: Slot<Vec<String>>,
    /// The device a retrieve is run on.
    retriever: Slot<String>,
    /// The devices the app crossed off.
    crossed_off: Slot<Vec<String>>,
    /// The device's notes and orders before the run.
    before: Slot<Vec<Vec<String>>>,
    /// What the app's call to retrieve returned.
    called: Slot<Record>,
}

#[fixture]
pub(crate) fn devices() -> Devices {
    Devices::default()
}

/// The shared key every device holds, as 64 hex digits.
const KEY: &str = "0707070707070707070707070707070707070707070707070707070707070707";

impl Devices {
    fn add(&self, world: &World, name: &str, sql: &str) -> Result<String, String> {
        let address = world.place(&format!("{name}.db"), sql)?;
        let mut places = self.places.take().unwrap_or_default();
        places.insert(name.to_owned(), address.clone());
        self.places.set(places);
        Ok(address)
    }

    fn place(&self, name: &str) -> Result<String, String> {
        self.places
            .get()
            .and_then(|places| places.get(name).cloned())
            .ok_or_else(|| format!("no device {name}"))
    }

    /// Devices `names`, each holding three notes and an empty orders table,
    /// in step, each stashed once.
    fn in_step(&self, world: &World, names: &[&str]) -> Result<(), String> {
        for name in names {
            self.add(world, name, &format!("{NOTES}; {THREE_NOTES}; {ORDERS}"))?;
            stash_at(world, &self.place(name)?, name, &[], &[])?;
        }
        Ok(())
    }

    /// Runs `sql` on device `name`.
    fn change(&self, name: &str, sql: &str) -> Result<(), String> {
        sql_on(&self.place(name)?, sql)
    }

    /// Stashes device `name` for `targets` and keeps the blob.
    fn stash(&self, world: &World, name: &str, targets: &[&str]) -> Result<String, String> {
        let blob = stash_at(world, &self.place(name)?, name, targets, &[])?;
        let mut blobs = self.blobs.take().unwrap_or_default();
        blobs.push(blob.clone());
        self.blobs.set(blobs);
        Ok(blob)
    }

    fn last_blob(&self) -> Result<String, String> {
        self.blobs
            .get()
            .and_then(|blobs| blobs.last().cloned())
            .ok_or_else(|| "no blob stashed".to_owned())
    }

    /// Retrieves `blob` on device `name` and returns its record.
    fn retrieve(&self, world: &World, name: &str, blob: &str) -> Result<Json, String> {
        let place = self.place(name)?;
        let crossed = self.crossed_off.get().unwrap_or_default();
        let mut args = vec![
            "retrieve",
            place.as_str(),
            blob,
            "--json",
            "--node",
            name,
            "--key-file",
        ];
        let key = key_file(world)?;
        args.push(&key);
        for device in &crossed {
            args.extend(["--crossed-off", device.as_str()]);
        }
        let output = world.smugglr(&args)?;
        serde_json::from_slice(&output.stdout).map_err(|err| {
            format!(
                "smugglr retrieve printed no record ({err}): {}",
                String::from_utf8_lossy(&output.stderr)
            )
        })
    }

    /// Fails unless every device in `names` holds the same rows.
    fn same_rows(&self, names: &[&str]) -> Result<(), String> {
        let mut first: Option<Vec<Vec<String>>> = None;
        for name in names {
            let rows = every_row(&self.place(name)?)?;
            match &first {
                Some(first) if *first != rows => {
                    return Err(format!(
                        "{name} holds {rows:?}, {} holds {first:?}",
                        names[0]
                    ));
                }
                Some(_) => {}
                None => first = Some(rows),
            }
        }
        Ok(())
    }
}

/// Every notes and orders row in the file at `path`.
fn every_row(path: &str) -> Result<Vec<Vec<String>>, String> {
    let mut rows = rows_of(path, "SELECT * FROM notes ORDER BY id")?;
    rows.extend(rows_of(path, "SELECT * FROM orders ORDER BY id")?);
    Ok(rows)
}

fn key_file(world: &World) -> Result<String, String> {
    let path = world.folder()?.join("mesh.key");
    std::fs::write(&path, KEY).map_err(|err| format!("{}: {err}", path.display()))?;
    Ok(path.display().to_string())
}

/// Runs smugglr stash on the place at `address` as device `node`, for
/// `targets`, with `flags`, and returns the blob's path.
fn stash_at(
    world: &World,
    address: &str,
    node: &str,
    targets: &[&str],
    flags: &[&str],
) -> Result<String, String> {
    let bucket = world.folder()?.join("bucket").display().to_string();
    let key = key_file(world)?;
    let mut args = vec![
        "stash",
        address,
        bucket.as_str(),
        "--node",
        node,
        "--key-file",
        key.as_str(),
    ];
    for target in targets {
        args.extend(["--target", target]);
    }
    args.extend_from_slice(flags);
    let output = world.smugglr(&args)?;
    if !output.status.success() {
        return Err(format!(
            "smugglr stash exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn blob_json(path: &str) -> Result<Json, String> {
    let text = std::fs::read_to_string(path).map_err(|err| format!("{path}: {err}"))?;
    serde_json::from_str(&text).map_err(|err| format!("{path}: {err}"))
}

/// Each change's key in the blob at `path`, as `quote()` text, with its op.
fn changes(path: &str) -> Result<Vec<(Vec<String>, String)>, String> {
    let blob = blob_json(path)?;
    let changes = blob["changes"]
        .as_array()
        .ok_or("the blob has no changes")?;
    Ok(changes
        .iter()
        .map(|change| {
            let key = change["key"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|column| column.as_str().map(str::to_owned))
                .collect();
            (key, change["op"].as_str().unwrap_or_default().to_owned())
        })
        .collect())
}

fn status(record: &Json) -> &str {
    record["status"].as_str().unwrap_or_default()
}

#[given("a device's database stashed before, and two rows since changed")]
fn two_rows_changed(world: &World, devices: &Devices) -> Result<(), String> {
    devices.in_step(world, &["a"])?;
    devices.retriever.set("a".to_owned());
    devices.change(
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'; \
         INSERT INTO notes VALUES ('n4', 'fourth', 4)",
    )
}

#[given("a device's database stashed before, and a row since deleted")]
fn a_row_deleted(world: &World, devices: &Devices) -> Result<(), String> {
    devices.in_step(world, &["a"])?;
    devices.retriever.set("a".to_owned());
    devices.change("a", "DELETE FROM notes WHERE id = 'n2'")
}

#[when("the agent runs smugglr stash on it")]
fn stash_it(world: &World, devices: &Devices) -> Result<(), String> {
    let name = devices.retriever.get().ok_or("no device")?;
    devices.stash(world, &name, &["b"]).map(|_| ())
}

#[then("the blob holds changes for exactly those two rows")]
fn holds_those_two(devices: &Devices) -> Result<(), String> {
    let blob = devices.last_blob()?;
    let mut keys: Vec<Vec<String>> = changes(&blob)?.into_iter().map(|(k, _)| k).collect();
    keys.sort();
    let expected = vec![vec!["'n1'".to_owned()], vec!["'n4'".to_owned()]];
    if keys == expected && blob_json(&blob)?["whole"] == Json::Bool(false) {
        Ok(())
    } else {
        Err(format!("the blob holds changes for {keys:?}"))
    }
}

#[then("the blob holds a delete for that row's key")]
fn holds_the_delete(devices: &Devices) -> Result<(), String> {
    let changes = changes(&devices.last_blob()?)?;
    if changes == [(vec!["'n2'".to_owned()], "down".to_owned())] {
        Ok(())
    } else {
        Err(format!("the blob holds {changes:?}"))
    }
}

#[given(
    "a blob stashed from one device, and a second device in step with the first before those changes"
)]
fn a_blob_and_a_second_device(world: &World, devices: &Devices) -> Result<(), String> {
    devices.in_step(world, &["a", "b"])?;
    devices.retriever.set("b".to_owned());
    devices.change(
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'; DELETE FROM notes WHERE id = 'n2'; \
         INSERT INTO notes VALUES ('n4', 'fourth', 4); INSERT INTO orders VALUES ('o1', 'tea')",
    )?;
    devices.stash(world, "a", &["b"]).map(|_| ())
}

#[when("the agent runs smugglr retrieve on the second device with that blob")]
#[when("the agent runs smugglr retrieve on the device with that blob")]
fn retrieve_it(world: &World, devices: &Devices) -> Result<(), String> {
    let name = devices.retriever.get().ok_or("no device")?;
    devices.retrieve(world, &name, &devices.last_blob()?)?;
    Ok(())
}

#[then("the second device holds the same rows as the first")]
fn second_holds_the_first(devices: &Devices) -> Result<(), String> {
    devices.same_rows(&["a", "b"])
}

#[given(
    "blobs stashed from two devices, each holding changes to rows the other also changed later"
)]
fn blobs_from_two_devices(world: &World, devices: &Devices) -> Result<(), String> {
    devices.in_step(world, &["a", "b", "c"])?;
    devices.change("a", "UPDATE notes SET body = 'a1' WHERE id = 'n1'")?;
    let first = devices.stash(world, "a", &["b", "c"])?;
    devices.retrieve(world, "b", &first)?;
    devices.change(
        "b",
        "UPDATE notes SET body = 'b1' WHERE id = 'n1'; UPDATE notes SET body = 'b2' WHERE id = 'n2'",
    )?;
    let second = devices.stash(world, "b", &["a", "c"])?;
    devices.retrieve(world, "a", &second)?;
    devices.change("a", "UPDATE notes SET body = 'a2' WHERE id = 'n2'")?;
    let third = devices.stash(world, "a", &["b", "c"])?;
    devices.retrieve(world, "b", &third)?;
    Ok(())
}

#[when("a third device retrieves the blobs in the opposite order to how they were stashed")]
fn third_in_opposite_order(world: &World, devices: &Devices) -> Result<(), String> {
    // As the app does: each blob that still lists this device is retrieved
    // again until it is done.
    let mut left: Vec<String> = devices.blobs.get().unwrap_or_default();
    left.reverse();
    let mut last = Vec::new();
    for _ in 0..=left.len() {
        let mut next = Vec::new();
        last.clear();
        for blob in left {
            let record = devices.retrieve(world, "c", &blob)?;
            if status(&record) != "done" {
                next.push(blob);
                last.push(record);
            }
        }
        left = next;
        if left.is_empty() {
            return Ok(());
        }
    }
    Err(format!("blobs never done: {last:?}"))
}

#[then("the third device holds the same rows as the first two")]
fn third_holds_the_first_two(devices: &Devices) -> Result<(), String> {
    devices.same_rows(&["a", "b", "c"])
}

#[given("a blob whose header's shape stamp for orders differs from the device's orders table")]
fn a_blob_of_another_shape(world: &World, devices: &Devices) -> Result<(), String> {
    devices.add(
        world,
        "a",
        &format!(
            "{NOTES}; {THREE_NOTES}; CREATE TABLE orders (id TEXT PRIMARY KEY, item TEXT, qty INTEGER)"
        ),
    )?;
    let b = devices.add(world, "b", &format!("{NOTES}; {ORDERS}"))?;
    devices.retriever.set("b".to_owned());
    devices.stash(world, "a", &["b"])?;
    devices.before.set(every_row(&b)?);
    Ok(())
}

#[then("no change of the blob is applied")]
fn none_applied(world: &World, devices: &Devices) -> Result<(), String> {
    let before = devices.before.get().unwrap_or_default();
    let now = every_row(&devices.place("b")?)?;
    let output = world.output.get().ok_or("smugglr was not run")?;
    let record: Json = serde_json::from_slice(&output.stdout).map_err(|err| err.to_string())?;
    if now != before {
        return Err(format!("the device now holds {now:?}"));
    }
    if record["shape"]["table"] != "orders" || output.status.code() != Some(3) {
        return Err(format!("retrieve exited {}: {record}", output.status));
    }
    Ok(())
}

#[given("a blob whose every change can be applied on the device")]
fn a_blob_that_applies(world: &World, devices: &Devices) -> Result<(), String> {
    a_blob_and_a_second_device(world, devices)
}

#[when("the app calls retrieve with that blob")]
fn the_app_calls_retrieve(world: &World, devices: &Devices) -> Result<(), String> {
    let path = devices.last_blob()?;
    let text = std::fs::read_to_string(&path).map_err(|err| format!("{path}: {err}"))?;
    let blob = Blob::from_json(&text).ok_or("not a blob")?;
    let mut place = LocalFile::open(&devices.place("b")?).map_err(|err| err.to_string())?;
    let mut key = [0; 32];
    key.fill(7);
    let settings = Settings {
        store: std::sync::Arc::new(smugglr_memory::Files::new(world.config()?)),
        key,
        node: "b".to_owned(),
        targets: Vec::new(),
        graveyard: GRAVEYARD,
        now: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| err.to_string())?
            .as_secs(),
        sync_mode: SyncMode::Stop,
        steering: smugglr_core::steering::Steering::default(),
        ignore: Vec::new(),
        max_value: smugglr_core::weight::DEFAULT_MAX_VALUE,
    };
    let record = smugglr_multi::retrieve(&mut place, &[blob], &settings, &[])
        .map_err(|err| err.to_string())?;
    devices.called.set(record);
    Ok(())
}

#[then("the record's status is done")]
fn status_done(devices: &Devices) -> Result<(), String> {
    let record = devices.called.get().ok_or("retrieve was not called")?;
    if record.status == Status::Done {
        Ok(())
    } else {
        Err(format!("the record is {}", record.json()))
    }
}

#[given("a device offline for longer than the sunset while the others changed rows")]
fn offline_past_the_sunset(world: &World, devices: &Devices) -> Result<(), String> {
    config(world, "graveyard = 2\n")?;
    devices.in_step(world, &["a", "b", "c"])?;
    devices.change(
        "a",
        "UPDATE notes SET body = 'edited' WHERE id = 'n1'; DELETE FROM notes WHERE id = 'n2'; \
         INSERT INTO notes VALUES ('n4', 'fourth', 4)",
    )?;
    let first = devices.stash(world, "a", &["b", "c"])?;
    devices.retrieve(world, "b", &first)?;
    devices.change("b", "UPDATE notes SET body = 'later' WHERE id = 'n3'")?;
    let second = devices.stash(world, "b", &["a", "c"])?;
    devices.retrieve(world, "a", &second)?;
    std::thread::sleep(Duration::from_secs(3));
    Ok(())
}

#[when("it retrieves a stash made from a device that stayed in step")]
fn retrieves_a_whole_stash(world: &World, devices: &Devices) -> Result<(), String> {
    let blob = stash_at(world, &devices.place("b")?, "b", &["c"], &["--whole"])?;
    let record = devices.retrieve(world, "c", &blob)?;
    if status(&record) == "done" {
        Ok(())
    } else {
        Err(format!("retrieve: {record}"))
    }
}

#[then("it holds the same rows as the others")]
fn holds_the_same_as_the_others(devices: &Devices) -> Result<(), String> {
    devices.same_rows(&["a", "b", "c"])
}

#[given("a place with rows, and a new place with the same tables and no rows")]
fn an_old_and_a_new_place(world: &World, devices: &Devices) -> Result<(), String> {
    devices.add(
        world,
        "old",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}; INSERT INTO orders VALUES ('o1', 'tea')"),
    )?;
    devices.add(world, "new", &format!("{NOTES}; {ORDERS}"))?;
    Ok(())
}

#[when(
    "the agent runs smugglr stash on the old place and smugglr retrieve on the new place with that stash"
)]
fn move_the_database(world: &World, devices: &Devices) -> Result<(), String> {
    devices.stash(world, "old", &[])?;
    devices.retrieve(world, "new", &devices.last_blob()?)?;
    Ok(())
}

#[then("the new place holds every row of the old one")]
fn new_holds_every_row(devices: &Devices) -> Result<(), String> {
    devices.same_rows(&["old", "new"])
}

#[given("a blob stashed for three devices, already crossed off by one")]
fn crossed_off_by_one(world: &World, devices: &Devices) -> Result<(), String> {
    devices.in_step(world, &["a"])?;
    devices.add(world, "c", &format!("{NOTES}; {ORDERS}"))?;
    devices.change("a", "UPDATE notes SET body = 'edited' WHERE id = 'n1'")?;
    devices.stash(world, "a", &["b", "c", "d"])?;
    devices.crossed_off.set(vec!["b".to_owned()]);
    Ok(())
}

#[when("a second device runs smugglr retrieve of that blob with --json")]
fn second_retrieves(world: &World, devices: &Devices) -> Result<(), String> {
    let record = devices.retrieve(world, "c", &devices.last_blob()?)?;
    devices.before.set(vec![
        record["targets_remaining"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|target| target.as_str().map(str::to_owned))
            .collect(),
    ]);
    Ok(())
}

#[then("the record names the third device as the target that remains")]
fn names_the_third(devices: &Devices) -> Result<(), String> {
    let remaining = devices.before.get().unwrap_or_default();
    if remaining == [vec!["d".to_owned()]] {
        Ok(())
    } else {
        Err(format!("the record names {remaining:?} as remaining"))
    }
}

#[given(
    "an HTTP-SQL service holding rows, and a second HTTP-SQL service with the same shape and no rows"
)]
fn two_services(world: &World, devices: &Devices) -> Result<(), String> {
    let first = service(
        world,
        "first.db",
        &format!("{NOTES}; {THREE_NOTES}; {ORDERS}"),
    )?;
    let second = service(world, "second.db", &format!("{NOTES}; {ORDERS}"))?;
    let folder = world.folder()?;
    let mut places = BTreeMap::new();
    places.insert("first".to_owned(), first);
    places.insert("second".to_owned(), second);
    places.insert(
        "first.db".to_owned(),
        folder.join("first.db").display().to_string(),
    );
    places.insert(
        "second.db".to_owned(),
        folder.join("second.db").display().to_string(),
    );
    devices.places.set(places);
    Ok(())
}

#[when("the agent runs smugglr stash from the first and smugglr retrieve into the second")]
fn stash_then_retrieve_services(world: &World, devices: &Devices) -> Result<(), String> {
    devices.stash(world, "first", &[])?;
    let record = devices.retrieve(world, "second", &devices.last_blob()?)?;
    if status(&record) == "done" {
        Ok(())
    } else {
        Err(format!("retrieve: {record}"))
    }
}

#[then("the second holds every row of the first")]
fn second_holds_every_row(devices: &Devices) -> Result<(), String> {
    devices.same_rows(&["first.db", "second.db"])
}
