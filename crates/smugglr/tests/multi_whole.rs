//! The NFR-SMUGGLR-006 measurement: three device databases kept in step by
//! smugglr stash and smugglr retrieve through a local directory standing for
//! the bucket, the graveyard set short so a sunset passes in seconds, with
//! inserts, edits and deletes on every device and no conflicting edits (each
//! device changes only rows it made), blobs retrieved in shuffled order; each
//! device is then compared with what the people meant.
//!
//! It runs in real time, about a minute, so it runs only when asked:
//! `cargo test -p smugglr --test multi_whole -- --ignored --nocapture`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

/// The graveyard window, in seconds: the sunset of every change.
const SUNSET: u64 = 10;

const DEVICES: [&str; 3] = ["d0", "d1", "d2"];

/// A small xorshift generator; the seed is printed so a run can be repeated.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = usize::try_from(self.below(i as u64 + 1)).unwrap_or(0);
            items.swap(i, j);
        }
    }
}

/// Rows by id: what a device holds, or what the people meant.
type Rows = BTreeMap<String, String>;

fn sql(path: &Path, statement: &str) -> Result<(), String> {
    Connection::open(path)
        .and_then(|c| c.execute_batch(statement))
        .map_err(|err| format!("{}: {err}", path.display()))
}

fn rows(path: &Path) -> Result<Rows, String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    let mut statement = connection
        .prepare("SELECT id, body FROM notes")
        .map_err(|err| err.to_string())?;
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .and_then(Iterator::collect)
        .map_err(|err| err.to_string())
}

/// A blob in the bucket: the devices it is for, and those crossed off.
#[derive(Debug)]
struct Stashed {
    maker: String,
    targets: Vec<String>,
    crossed: BTreeSet<String>,
}

struct Run {
    folder: PathBuf,
    random: Random,
    meant: Rows,
    made: usize,
    bucket: BTreeMap<PathBuf, Stashed>,
}

impl Run {
    fn new(folder: PathBuf, seed: u64) -> Result<Self, String> {
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(folder.join("config/smugglr")).map_err(|err| err.to_string())?;
        std::fs::write(
            folder.join("config/smugglr/config.toml"),
            format!("graveyard = {SUNSET}\n"),
        )
        .map_err(|err| err.to_string())?;
        std::fs::write(folder.join("multi.key"), "07".repeat(32)).map_err(|err| err.to_string())?;
        let mut run = Self {
            folder,
            random: Random(seed),
            meant: Rows::new(),
            made: 0,
            bucket: BTreeMap::new(),
        };
        for device in DEVICES {
            sql(
                &run.place(device),
                "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);",
            )?;
            // Each device's first stash is whole; the devices start in step.
            run.stash(device, false)?;
        }
        run.bucket.clear();
        Ok(run)
    }

    fn place(&self, device: &str) -> PathBuf {
        self.folder.join(format!("{device}.db"))
    }

    fn smugglr(&self, args: &[&str]) -> Result<std::process::Output, String> {
        Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .args(args)
            .env("XDG_CONFIG_HOME", self.folder.join("config"))
            .output()
            .map_err(|err| err.to_string())
    }

    /// Inserts, edits and deletes rows `device` made, in its database and in
    /// what the people meant. A deleted id is never made again.
    fn change(&mut self, device: &str) -> Result<(), String> {
        let mut statements = String::new();
        let own: Vec<String> = self
            .meant
            .keys()
            .filter(|id| id.starts_with(device))
            .cloned()
            .collect();
        for _ in 0..self.random.below(10) {
            let pick = usize::try_from(self.random.below(own.len() as u64)).unwrap_or(0);
            match (self.random.below(3), own.get(pick)) {
                (0, Some(id)) if self.meant.contains_key(id) => {
                    let _ = write!(statements, "DELETE FROM notes WHERE id = '{id}';");
                    self.meant.remove(id);
                }
                (1, Some(id)) if self.meant.contains_key(id) => {
                    let body = format!("edit {}", self.random.next());
                    let _ = write!(
                        statements,
                        "UPDATE notes SET body = '{body}' WHERE id = '{id}';"
                    );
                    self.meant.insert(id.clone(), body);
                }
                _ => {
                    self.made += 1;
                    let id = format!("{device}-{:05}", self.made);
                    let body = format!("new {}", self.random.next());
                    let _ = write!(statements, "INSERT INTO notes VALUES ('{id}', '{body}');");
                    self.meant.insert(id, body);
                }
            }
        }
        sql(&self.place(device), &statements)
    }

    /// Stashes `device` for the other devices and puts the blob in the bucket.
    fn stash(&mut self, device: &str, whole: bool) -> Result<PathBuf, String> {
        let targets: Vec<String> = DEVICES
            .iter()
            .filter(|other| **other != device)
            .map(|other| (*other).to_owned())
            .collect();
        self.stash_for(device, &targets, whole)
    }

    fn stash_for(
        &mut self,
        device: &str,
        targets: &[String],
        whole: bool,
    ) -> Result<PathBuf, String> {
        let place = self.place(device).display().to_string();
        let bucket = self.folder.join("bucket").display().to_string();
        let key = self.folder.join("multi.key").display().to_string();
        let mut args = vec![
            "stash",
            place.as_str(),
            bucket.as_str(),
            "--node",
            device,
            "--key-file",
            key.as_str(),
        ];
        for target in targets {
            args.extend(["--target", target.as_str()]);
        }
        if whole {
            args.push("--whole");
        }
        let output = self.smugglr(&args)?;
        if !output.status.success() {
            return Err(format!(
                "stash exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let blob = PathBuf::from(String::from_utf8_lossy(&output.stdout).trim());
        self.bucket.insert(
            blob.clone(),
            Stashed {
                maker: device.to_owned(),
                targets: targets.to_vec(),
                crossed: BTreeSet::new(),
            },
        );
        Ok(blob)
    }

    /// Retrieves, in shuffled order, the blobs that still list `device`,
    /// each with probability `chance` in 4; crosses `device` off each that is
    /// done, and deletes a blob with no targets left.
    fn retrieve(&mut self, device: &str, chance: u64) -> Result<(), String> {
        let mut listed: Vec<PathBuf> = self
            .bucket
            .iter()
            .filter(|(_, blob)| {
                blob.targets.iter().any(|t| t == device) && !blob.crossed.contains(device)
            })
            .map(|(path, _)| path.clone())
            .collect();
        self.random.shuffle(&mut listed);
        for path in listed {
            if self.random.below(4) >= chance {
                continue;
            }
            if self.retrieve_one(device, &path)? {
                self.cross_off(device, &path)?;
            }
        }
        Ok(())
    }

    /// Retrieves one blob on `device`; whether the record is done.
    fn retrieve_one(&self, device: &str, path: &Path) -> Result<bool, String> {
        let place = self.place(device).display().to_string();
        let blob = path.display().to_string();
        let key = self.folder.join("multi.key").display().to_string();
        let crossed: Vec<String> = self
            .bucket
            .get(path)
            .map(|blob| blob.crossed.iter().cloned().collect())
            .unwrap_or_default();
        let mut args = vec![
            "retrieve",
            place.as_str(),
            blob.as_str(),
            "--node",
            device,
            "--key-file",
            key.as_str(),
            "--json",
        ];
        for other in &crossed {
            args.extend(["--crossed-off", other.as_str()]);
        }
        let output = self.smugglr(&args)?;
        let record: serde_json::Value = serde_json::from_slice(&output.stdout).map_err(|err| {
            format!(
                "retrieve printed no record ({err}): {}",
                String::from_utf8_lossy(&output.stderr)
            )
        })?;
        Ok(record["status"] == "done")
    }

    fn cross_off(&mut self, device: &str, path: &Path) -> Result<(), String> {
        let Some(blob) = self.bucket.get_mut(path) else {
            return Ok(());
        };
        blob.crossed.insert(device.to_owned());
        if blob.targets.iter().all(|t| blob.crossed.contains(t)) {
            self.bucket.remove(path);
            std::fs::remove_file(path).map_err(|err| err.to_string())?;
        }
        Ok(())
    }

    /// One round: each device in `online` changes rows (when `changing`),
    /// stashes and retrieves; the devices offline change rows only.
    fn round(&mut self, online: &[&str], changing: bool, chance: u64) -> Result<(), String> {
        for device in DEVICES {
            if changing {
                self.change(device)?;
            }
            if online.contains(&device) {
                self.stash(device, false)?;
            }
        }
        for device in online {
            self.retrieve(device, chance)?;
        }
        Ok(())
    }

    /// `device` comes back after `away`: past the sunset it catches up from a
    /// whole stash made by a device that stayed in step.
    fn come_back(&mut self, device: &str, away: Duration) -> Result<(), String> {
        if away > Duration::from_secs(SUNSET) {
            let helper = DEVICES
                .iter()
                .find(|other| **other != device)
                .copied()
                .unwrap_or("d0");
            let whole = self.stash_for(helper, &[device.to_owned()], true)?;
            if !self.retrieve_one(device, &whole)? {
                return Err("the whole stash was not done".to_owned());
            }
            // The whole stash holds what the helper made or had retrieved,
            // so the app crosses the device off those blobs too.
            let covered: Vec<PathBuf> = self
                .bucket
                .iter()
                .filter(|(_, blob)| blob.maker == helper || blob.crossed.contains(helper))
                .map(|(path, _)| path.clone())
                .collect();
            for path in covered {
                self.cross_off(device, &path)?;
            }
        }
        self.stash(device, false)?;
        self.retrieve(device, 4)
    }

    /// Rounds with no changes until every blob is crossed off and deleted,
    /// or two sunsets pass.
    fn settle(&mut self) -> Result<(), String> {
        let started = Instant::now();
        while !self.bucket.is_empty() && started.elapsed() < Duration::from_secs(2 * SUNSET) {
            self.round(&DEVICES, false, 4)?;
            sleep(Duration::from_millis(200));
        }
        // Stashes made while settling hold nothing; the last ones may be
        // left uncrossed by a device that already holds every row.
        Ok(())
    }

    fn harm(&self) -> Result<Harm, String> {
        let mut harm = Harm::default();
        for device in DEVICES {
            let held = rows(&self.place(device))?;
            for (id, body) in &self.meant {
                match held.get(id) {
                    None => harm.rows_lost += 1,
                    Some(found) if found != body => harm.edits_lost += 1,
                    Some(_) => {}
                }
            }
            harm.deletes_undone += held
                .keys()
                .filter(|id| !self.meant.contains_key(*id))
                .count();
        }
        Ok(harm)
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Harm {
    rows_lost: usize,
    deletes_undone: usize,
    edits_lost: usize,
}

fn seed() -> Result<u64, String> {
    let nanos: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .subsec_nanos()
        .into();
    Ok(nanos | 1)
}

/// Runs rounds for `away`, `offline` changing rows but neither stashing nor
/// retrieving, then brings it back, settles, and measures.
fn offline_for(name: &str, away: Duration) -> Result<Harm, String> {
    let seed = seed()?;
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("multi-whole-{name}"));
    let mut run = Run::new(folder, seed)?;
    for _ in 0..4 {
        run.round(&DEVICES, true, 4)?;
    }
    let started = Instant::now();
    while started.elapsed() < away {
        run.round(&["d0", "d1"], true, 4)?;
        sleep(Duration::from_millis(300));
    }
    run.come_back("d2", started.elapsed())?;
    for _ in 0..4 {
        run.round(&DEVICES, true, 4)?;
    }
    run.settle()?;
    let harm = run.harm()?;
    println!(
        "{name} (seed {seed}): {harm:?}, {} rows meant",
        run.meant.len()
    );
    Ok(harm)
}

#[test]
#[ignore = "the NFR-SMUGGLR-006 measurement: real time, about a minute"]
fn multi_stays_whole() -> Result<(), String> {
    let inside = offline_for(
        "offline two thirds of the sunset",
        Duration::from_secs(SUNSET * 2 / 3),
    )?;
    let past = offline_for("offline five sunsets", Duration::from_secs(5 * SUNSET))?;
    let seed = seed()?;
    let folder = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("multi-whole-any-order");
    let mut run = Run::new(folder, seed)?;
    for _ in 0..12 {
        // Each blob is retrieved in a round with chance 1 in 2, so blobs
        // arrive across rounds in any order.
        run.round(&DEVICES, true, 2)?;
    }
    run.settle()?;
    let any_order = run.harm()?;
    println!(
        "blobs in any order (seed {seed}): {any_order:?}, {} rows meant",
        run.meant.len()
    );
    let none = Harm::default();
    if inside == none && past == none && any_order == none {
        Ok(())
    } else {
        Err(format!(
            "inside {inside:?}, past {past:?}, any order {any_order:?}"
        ))
    }
}
