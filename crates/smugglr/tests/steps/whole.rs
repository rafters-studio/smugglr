//! Steps for NFR-SMUGGLR-004: the built smugglr binary syncs two local files
//! 200 times, with inserts, edits and deletes on both sides between runs, and
//! after every run both files are held against a reference of what they
//! should hold.
//!
//! No key is changed on both sides between the same two runs: such a row
//! follows --sync-mode, which is not what this measures. smugglr reads no
//! clock, so a clock behind lives where a writer leaves it: in the rows it
//! stamps and in its file's modified time.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use rusqlite::types::Value;
use rusqlite::{Connection, params_from_iter};

use crate::setup::World;

/// The runs each condition is measured over.
const RUNS: usize = 200;

/// No steering column, and a key smugglr can write: `INT`, not `INTEGER`,
/// so the key is not the rowid the database hands out (#507).
const ITEMS: &str = "CREATE TABLE items (id INT PRIMARY KEY, body TEXT, n INTEGER, x REAL, \
                     b BLOB, stamp INTEGER)";

const INSERT: &str = "INSERT INTO items VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const EDIT: &str = "UPDATE items SET body = ?2, n = ?3, x = ?4, b = ?5, stamp = ?6 WHERE id = ?1";
const DELETE: &str = "DELETE FROM items WHERE id = ?1";

/// How the keys of new rows are chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keys {
    /// 1, 2, 3, and on.
    InOrder,
    /// Any 64-bit integer, in no order.
    Random,
}

/// One of the four conditions the NFR names.
#[derive(Debug, Clone, Copy)]
struct Condition {
    keys: Keys,
    /// Seconds the target's writer's clock is behind.
    behind: u64,
    /// Rows are committed into the source while each run reads.
    mid_run: bool,
    seed: u64,
}

/// What the runs lost, against the reference.
#[derive(Debug, Clone, Default)]
struct Tally {
    runs: usize,
    lost: usize,
    undone: usize,
    edits_lost: usize,
    /// Rows a place holds that no writer wrote.
    strays: usize,
    /// Runs smugglr did not finish with exit 0.
    failed: usize,
    /// Rows committed into the source while a run was going.
    committed_mid_run: usize,
    /// Runs during which no row was committed.
    runs_without_commit: usize,
    /// The first few of what went wrong, to name in the failure.
    notes: Vec<String>,
}

impl Tally {
    fn note(&mut self, note: String) {
        if self.notes.len() < 10 {
            self.notes.push(note);
        }
    }
}

/// The scenario's condition and, once run, its tally.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Soak {
    condition: Slot<Condition>,
    tally: Slot<Tally>,
}

#[fixture]
pub(crate) fn soak() -> Soak {
    Soak::default()
}

/// splitmix64: the same seed gives the same runs, so a failure can be
/// replayed.
#[derive(Debug, Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, from: &[T]) -> Option<T> {
        if from.is_empty() {
            return None;
        }
        let at = usize::try_from(self.below(from.len() as u64)).ok()?;
        from.get(at).copied()
    }
}

/// Hands out keys never used before in the scenario, so a deleted key is
/// never written again and a row holding it is a delete undone.
#[derive(Debug)]
struct KeyMaker {
    keys: Keys,
    next: i64,
    used: BTreeSet<i64>,
    rng: Rng,
}

impl KeyMaker {
    fn fresh(&mut self) -> i64 {
        loop {
            let key = match self.keys {
                Keys::InOrder => {
                    self.next += 1;
                    self.next
                }
                Keys::Random => i64::from_ne_bytes(self.rng.next().to_ne_bytes()),
            };
            if self.used.insert(key) {
                return key;
            }
        }
    }
}

/// One row's values, in the table's column order.
type Row = Vec<Value>;

/// A write committed while a run went, and what its key held before.
type Committed = (Write, Option<Row>);

fn new_row(id: i64, stamp: i64, rng: &mut Rng) -> Row {
    let body = if rng.below(8) == 0 {
        Value::Null
    } else {
        Value::Text(format!("body {:x}", rng.next()))
    };
    let blob: Vec<u8> = (0..rng.below(9))
        .map(|_| rng.next().to_le_bytes()[0])
        .collect();
    let x = f64::from(u32::try_from(rng.below(4000)).unwrap_or(0)) / 4.0;
    Row::from([
        Value::Integer(id),
        body,
        Value::Integer(i64::from_ne_bytes(rng.next().to_ne_bytes())),
        Value::Real(x),
        Value::Blob(blob),
        Value::Integer(stamp),
    ])
}

/// What a writer does to its file.
#[derive(Debug, Clone)]
enum Write {
    Insert(Row),
    Edit(Row),
    Delete(i64),
}

impl Write {
    fn key(&self) -> i64 {
        match self {
            Self::Insert(row) | Self::Edit(row) => match row.first() {
                Some(Value::Integer(key)) => *key,
                _ => unreachable!("every row starts with its integer key"),
            },
            Self::Delete(key) => *key,
        }
    }

    fn run(&self, connection: &Connection) -> rusqlite::Result<usize> {
        match self {
            Self::Insert(row) => connection.execute(INSERT, params_from_iter(row.iter())),
            Self::Edit(row) => connection.execute(EDIT, params_from_iter(row.iter())),
            Self::Delete(key) => connection.execute(DELETE, [key]),
        }
    }
}

/// What both files should hold, kept from every write the writers made.
#[derive(Debug, Default)]
struct Reference {
    rows: BTreeMap<i64, Row>,
    deleted: BTreeSet<i64>,
}

impl Reference {
    fn apply(&mut self, write: &Write) {
        match write {
            Write::Insert(row) | Write::Edit(row) => {
                self.rows.insert(write.key(), row.clone());
            }
            Write::Delete(key) => {
                self.rows.remove(key);
                self.deleted.insert(*key);
            }
        }
    }

    /// The keys a writer may edit or delete: held, and not in `touched`.
    fn open_keys(&self, touched: &BTreeSet<i64>) -> Vec<i64> {
        self.rows
            .keys()
            .filter(|key| !touched.contains(key))
            .copied()
            .collect()
    }
}

/// A place's writer, its clock `behind` seconds behind this machine's.
#[derive(Debug, Clone)]
struct Writer {
    path: String,
    behind: u64,
}

impl Writer {
    fn now(&self) -> SystemTime {
        SystemTime::now() - Duration::from_secs(self.behind)
    }

    fn stamp(&self) -> i64 {
        self.now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|since| i64::try_from(since.as_secs()).ok())
            .unwrap_or(0)
    }

    fn connect(&self) -> Result<Connection, String> {
        let connection = Connection::open(&self.path).map_err(|err| self.err(&err))?;
        connection
            .busy_timeout(Duration::from_secs(10))
            .map_err(|err| self.err(&err))?;
        Ok(connection)
    }

    fn err(&self, err: &dyn std::fmt::Display) -> String {
        format!("{}: {err}", self.path)
    }

    /// Commits `writes` as one transaction, and leaves the file modified at
    /// the writer's own time.
    fn commit(&self, writes: &[Write]) -> Result<(), String> {
        let mut connection = self.connect()?;
        let transaction = connection.transaction().map_err(|err| self.err(&err))?;
        for write in writes {
            write.run(&transaction).map_err(|err| self.err(&err))?;
        }
        transaction.commit().map_err(|err| self.err(&err))?;
        drop(connection);
        std::fs::File::options()
            .write(true)
            .open(&self.path)
            .and_then(|file| file.set_modified(self.now()))
            .map_err(|err| self.err(&err))
    }
}

/// Every row the file at `path` holds, by key.
fn held(path: &str) -> Result<BTreeMap<i64, Row>, String> {
    let connection = Connection::open(path).map_err(|err| format!("{path}: {err}"))?;
    let mut statement = connection
        .prepare("SELECT id, body, n, x, b, stamp FROM items")
        .map_err(|err| format!("{path}: {err}"))?;
    let rows = statement
        .query_map([], |row| {
            (0..6)
                .map(|i| row.get::<_, Value>(i))
                .collect::<rusqlite::Result<Row>>()
        })
        .and_then(Iterator::collect::<rusqlite::Result<Vec<Row>>>)
        .map_err(|err| format!("{path}: {err}"))?;
    rows.into_iter()
        .map(|row| match row.first() {
            Some(Value::Integer(key)) => Ok((*key, row)),
            other => Err(format!("{path}: a row keyed {other:?}")),
        })
        .collect()
}

/// Holds the file at `path` against the reference after run `run`. A key in
/// `pending` was written while the run went: the file may still hold what
/// it held before (the value given), to be carried by the next run.
fn check(
    tally: &mut Tally,
    run: usize,
    side: &str,
    path: &str,
    reference: &Reference,
    pending: &BTreeMap<i64, Option<Row>>,
) -> Result<(), String> {
    let held = held(path)?;
    let keys: BTreeSet<i64> = reference.rows.keys().chain(held.keys()).copied().collect();
    for key in keys {
        let want = reference.rows.get(&key);
        let got = held.get(&key);
        if got == want
            || pending
                .get(&key)
                .is_some_and(|before| got == before.as_ref())
        {
            continue;
        }
        let what = match (want, got) {
            (Some(_), None) => {
                tally.lost += 1;
                "lost"
            }
            (None, Some(_)) if reference.deleted.contains(&key) => {
                tally.undone += 1;
                "a delete undone"
            }
            (None, Some(_)) => {
                tally.strays += 1;
                "a row no writer wrote"
            }
            _ => {
                tally.edits_lost += 1;
                "an edit lost"
            }
        };
        tally.note(format!(
            "run {run}: the {side} has {what} at key {key}: wanted {want:?}, held {got:?}"
        ));
    }
    Ok(())
}

/// The scenario's two places, writers and reference.
#[derive(Debug)]
struct Pair {
    source: Writer,
    target: Writer,
    reference: Reference,
    keys: KeyMaker,
    rng: Rng,
}

impl Pair {
    /// One side's inserts, edits and deletes between two runs, on keys not
    /// in `touched`; the keys it writes join `touched`.
    fn writes(&mut self, stamp: i64, touched: &mut BTreeSet<i64>) -> Vec<Write> {
        let mut writes = Vec::new();
        for _ in 0..self.rng.below(4) {
            let key = self.keys.fresh();
            writes.push(Write::Insert(new_row(key, stamp, &mut self.rng)));
        }
        for _ in 0..self.rng.below(4) {
            if let Some(key) = self.rng.pick(&self.reference.open_keys(touched)) {
                touched.insert(key);
                writes.push(Write::Edit(new_row(key, stamp, &mut self.rng)));
            }
        }
        for _ in 0..self.rng.below(3) {
            if let Some(key) = self.rng.pick(&self.reference.open_keys(touched)) {
                touched.insert(key);
                writes.push(Write::Delete(key));
            }
        }
        for write in &writes {
            touched.insert(write.key());
            self.reference.apply(write);
        }
        writes
    }
}

fn given(soak: &Soak, keys: Keys, behind: u64, seed: u64) {
    soak.condition.set(Condition {
        keys,
        behind,
        mid_run: false,
        seed,
    });
}

#[given("two local files with no steering column, keyed in order, whose writers' clocks agree")]
fn in_order_clocks_agree(soak: &Soak) {
    given(soak, Keys::InOrder, 0, 0x0004_0001);
}

#[given(
    "two local files with no steering column, keyed in order, the target's writer's clock 60 seconds behind"
)]
fn in_order_clock_behind(soak: &Soak) {
    given(soak, Keys::InOrder, 60, 0x0004_0002);
}

#[given(
    "two local files with no steering column, keyed by random 64-bit keys, whose writers' clocks agree"
)]
fn random_keys(soak: &Soak) {
    given(soak, Keys::Random, 0, 0x0004_0003);
}

/// Makes the two files with rows of their own, and syncs them once: both
/// then hold every row, and memory knows them.
fn first_sync(world: &World, condition: Condition) -> Result<Pair, String> {
    let mut rng = Rng(condition.seed);
    let mut keys = KeyMaker {
        keys: condition.keys,
        next: 0,
        used: BTreeSet::new(),
        rng: Rng(condition.seed ^ 0x5eed),
    };
    let source = Writer {
        path: world.place("source.db", ITEMS)?,
        behind: 0,
    };
    let target = Writer {
        path: world.place("target.db", ITEMS)?,
        behind: condition.behind,
    };
    // Enough rows that a read lasts while rows are committed during it.
    let rows = if condition.mid_run { 1000 } else { 300 };
    let mut reference = Reference::default();
    for (writer, count) in [(&source, rows), (&target, rows / 3)] {
        let mut writes: Vec<Write> = Vec::new();
        // The ends of the range, and either side of zero, in the source.
        if condition.keys == Keys::Random && writer.path == source.path {
            for key in [i64::MIN, i64::MAX, 0, -1] {
                keys.used.insert(key);
                writes.push(Write::Insert(new_row(key, writer.stamp(), &mut rng)));
            }
        }
        for _ in 0..count {
            let key = keys.fresh();
            writes.push(Write::Insert(new_row(key, writer.stamp(), &mut rng)));
        }
        writer.commit(&writes)?;
        for write in &writes {
            reference.apply(write);
        }
    }
    world.source.set(source.path.clone());
    world.target.set(target.path.clone());
    world.sync_ok()?;
    let mut tally = Tally::default();
    for (side, writer) in [("source", &source), ("target", &target)] {
        check(
            &mut tally,
            0,
            side,
            &writer.path,
            &reference,
            &BTreeMap::new(),
        )?;
    }
    if tally.notes.is_empty() {
        Ok(Pair {
            source,
            target,
            reference,
            keys,
            rng,
        })
    } else {
        Err(format!("the first sync: {}", tally.notes.join("; ")))
    }
}

/// Runs `smugglr sync` on the pair, committing rows into the source until
/// it exits. Returns what smugglr printed and the writes committed, each
/// with what the key held before it.
fn run_with_commits(
    world: &World,
    pair: &mut Pair,
    touched: &BTreeSet<i64>,
) -> Result<(Output, Vec<Committed>), String> {
    let child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["sync", pair.source.path.as_str(), pair.target.path.as_str()])
        .env("XDG_CONFIG_HOME", world.xdg()?)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    let exited = Arc::new(AtomicBool::new(false));
    let writer = pair.source.clone();
    let mut open = pair.reference.open_keys(touched);
    let mut keys = std::mem::replace(
        &mut pair.keys,
        KeyMaker {
            keys: Keys::InOrder,
            next: 0,
            used: BTreeSet::new(),
            rng: Rng(0),
        },
    );
    let mut rng = Rng(pair.rng.next());
    let done = Arc::clone(&exited);
    let writing = thread::spawn(move || -> Result<(Vec<Write>, KeyMaker), String> {
        let connection = writer.connect()?;
        let mut committed = Vec::new();
        while !done.load(Ordering::Acquire) {
            let stamp = writer.stamp();
            let edit = rng.below(10);
            let write = match (edit, open.is_empty()) {
                (5.., false) => {
                    let at = usize::try_from(rng.below(open.len() as u64)).unwrap_or(0);
                    let key = open.swap_remove(at);
                    if edit < 8 {
                        Write::Edit(new_row(key, stamp, &mut rng))
                    } else {
                        Write::Delete(key)
                    }
                }
                _ => Write::Insert(new_row(keys.fresh(), stamp, &mut rng)),
            };
            write.run(&connection).map_err(|err| writer.err(&err))?;
            committed.push(write);
            thread::sleep(Duration::from_micros(500));
        }
        Ok((committed, keys))
    });
    let output = child.wait_with_output();
    exited.store(true, Ordering::Release);
    let (committed, keys) = writing
        .join()
        .map_err(|_| "the committing writer panicked".to_owned())??;
    pair.keys = keys;
    let output = output.map_err(|err| format!("smugglr did not finish: {err}"))?;
    let committed = committed
        .into_iter()
        .map(|write| {
            let before = pair.reference.rows.get(&write.key()).cloned();
            pair.reference.apply(&write);
            (write, before)
        })
        .collect();
    Ok((output, committed))
}

fn runs(soak: &Soak, world: &World) -> Result<(), String> {
    let condition = soak.condition.get().ok_or("no condition given")?;
    let mut pair = first_sync(world, condition)?;
    let mut tally = Tally::default();
    // Keys written while the last run went, not yet sure to be carried.
    let mut pending: BTreeMap<i64, Option<Row>> = BTreeMap::new();
    for run in 1..=RUNS {
        let mut touched: BTreeSet<i64> = pending.keys().copied().collect();
        let source_writes = pair.writes(pair.source.stamp(), &mut touched);
        pair.source.commit(&source_writes)?;
        let target_writes = pair.writes(pair.target.stamp(), &mut touched);
        pair.target.commit(&target_writes)?;
        let output = if condition.mid_run {
            let (output, writes) = run_with_commits(world, &mut pair, &touched)?;
            tally.committed_mid_run += writes.len();
            if writes.is_empty() {
                tally.runs_without_commit += 1;
            }
            pending = writes
                .into_iter()
                .map(|(write, before)| (write.key(), before))
                .collect();
            output
        } else {
            world.sync(&[])?
        };
        tally.runs += 1;
        if !output.status.success() {
            tally.failed += 1;
            tally.note(format!(
                "run {run}: smugglr sync exited {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        // The source took the commits itself: it holds them whatever the run.
        check(
            &mut tally,
            run,
            "source",
            &pair.source.path,
            &pair.reference,
            &BTreeMap::new(),
        )?;
        check(
            &mut tally,
            run,
            "target",
            &pair.target.path,
            &pair.reference,
            &pending,
        )?;
    }
    // One run with no writer going carries what the last run could not.
    let settle = world.sync(&[])?;
    if !settle.status.success() {
        tally.failed += 1;
        tally.note(format!("the settling run exited {}", settle.status));
    }
    for (side, writer) in [("source", &pair.source), ("target", &pair.target)] {
        check(
            &mut tally,
            RUNS + 1,
            side,
            &writer.path,
            &pair.reference,
            &BTreeMap::new(),
        )?;
    }
    soak.tally.set(tally);
    Ok(())
}

#[when(
    "the agent runs smugglr sync on them 200 times, with inserts, edits and deletes on both sides between runs"
)]
fn two_hundred_runs(soak: &Soak, world: &World) -> Result<(), String> {
    runs(soak, world)
}

#[when(
    "the agent runs smugglr sync on them 200 times, with inserts, edits and deletes on both sides between runs, and rows committed into the source while each run reads"
)]
fn two_hundred_runs_with_commits(soak: &Soak, world: &World) -> Result<(), String> {
    soak.condition
        .with_mut(|condition| {
            condition.mid_run = true;
            condition.seed ^= 0x0004_0004;
        })
        .ok_or("no condition given")?;
    runs(soak, world)
}

#[then("against the reference, 0 rows are lost, 0 deletes undone and 0 edits lost")]
fn nothing_lost(soak: &Soak) -> Result<(), String> {
    let condition = soak.condition.get().ok_or("no condition given")?;
    let tally = soak.tally.get().ok_or("the runs were not made")?;
    let mut wrong = String::new();
    if tally.runs != RUNS {
        let _ = write!(wrong, "{} runs, not {RUNS}; ", tally.runs);
    }
    if condition.mid_run && tally.runs_without_commit > 0 {
        let _ = write!(
            wrong,
            "{} runs had no row committed while they went; ",
            tally.runs_without_commit
        );
    }
    if tally.lost + tally.undone + tally.edits_lost + tally.strays + tally.failed > 0 {
        let _ = write!(
            wrong,
            "{} rows lost, {} deletes undone, {} edits lost, {} rows no writer wrote, \
             {} runs failed (seed {:#x}): {}",
            tally.lost,
            tally.undone,
            tally.edits_lost,
            tally.strays,
            tally.failed,
            condition.seed,
            tally.notes.join("; ")
        );
    }
    if wrong.is_empty() { Ok(()) } else { Err(wrong) }
}
