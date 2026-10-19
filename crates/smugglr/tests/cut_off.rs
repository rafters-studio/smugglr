//! The NFR-SMUGGLR-003 measurement: smugglr sync runs between two local
//! files, each killed (SIGKILL) at a random point, with rows inserted,
//! edited and deleted on both sides before and after the cut, each followed
//! by a rerun; both files are then compared with what the people meant.
//!
//! It takes minutes, so it runs only when asked:
//! `cargo test -p smugglr --test cut_off -- --ignored --nocapture`.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

const RUNS: usize = 2_000;
const ROWS: usize = 200;

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
        self.next() % n
    }
}

/// Rows by id: what a place holds, or what the people meant.
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

/// Inserts, edits and deletes rows of `side`'s own ids (each side changes
/// only its own ids, so no row is changed in both places), in the place and
/// in what the people meant.
fn change(
    random: &mut Random,
    path: &Path,
    side: &str,
    meant: &mut Rows,
    round: usize,
) -> Result<(), String> {
    let mut statements = String::new();
    for _ in 0..random.below(12) {
        let id = format!("{side}{:03}", random.below(ROWS as u64));
        let body = format!("{side} round {round} {}", random.next());
        if meant.contains_key(&id) && random.below(3) == 0 {
            let _ = write!(statements, "DELETE FROM notes WHERE id = '{id}';");
            meant.remove(&id);
        } else {
            let _ = write!(
                statements,
                "INSERT INTO notes VALUES ('{id}', '{body}') \
                 ON CONFLICT (id) DO UPDATE SET body = excluded.body;"
            );
            meant.insert(id, body);
        }
    }
    sql(path, &statements)
}

fn smugglr(folder: &Path, source: &Path, target: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_smugglr"));
    command
        .arg("sync")
        .arg(source)
        .arg(target)
        .env("XDG_CONFIG_HOME", folder.join("config"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn sync_until_done(folder: &Path, source: &Path, target: &Path) -> Result<(), String> {
    let status = smugglr(folder, source, target)
        .status()
        .map_err(|err| err.to_string())?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("the rerun exited {status}"))
    }
}

#[derive(Debug, Default)]
struct Harm {
    deletes_undone: usize,
    rows_lost: usize,
    edits_lost: usize,
    reruns_not_done: usize,
}

fn compare(meant: &Rows, held: &Rows, harm: &mut Harm) {
    for (id, body) in meant {
        match held.get(id) {
            None => harm.rows_lost += 1,
            Some(found) if found != body => harm.edits_lost += 1,
            Some(_) => {}
        }
    }
    harm.deletes_undone += held.keys().filter(|id| !meant.contains_key(*id)).count();
}

#[test]
#[ignore = "the NFR-SMUGGLR-003 measurement: 2,000 killed runs, minutes long"]
fn runs_killed_at_random_points_do_no_harm() -> Result<(), String> {
    let seed: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| err.to_string())?
        .subsec_nanos()
        .into();
    let seed = seed | 1;
    println!("seed {seed}");
    let mut random = Random(seed);
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("cut-off");
    let mut harm = Harm::default();
    let mut longest = Duration::from_millis(20);
    for run in 0..RUNS {
        let folder = root.join(format!("{}-{run}", std::process::id()));
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
        let (source, target) = (folder.join("source.db"), folder.join("target.db"));
        let table = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT);";
        sql(&source, table)?;
        sql(&target, table)?;
        let mut meant = Rows::new();
        let (mut ours, mut theirs) = (Rows::new(), Rows::new());
        // A first sync, so the pair has memory and deletes are seen.
        change(&mut random, &source, "s", &mut ours, 0)?;
        change(&mut random, &target, "t", &mut theirs, 0)?;
        let first = Instant::now();
        sync_until_done(&folder, &source, &target)?;
        longest = longest.max(first.elapsed());
        change(&mut random, &source, "s", &mut ours, 1)?;
        change(&mut random, &target, "t", &mut theirs, 1)?;
        // The run, killed at a random point within the time a run takes.
        let mut child = smugglr(&folder, &source, &target)
            .spawn()
            .map_err(|err| err.to_string())?;
        let budget = u64::try_from(longest.as_micros()).unwrap_or(u64::MAX);
        sleep(Duration::from_micros(random.below(budget)));
        let _ = child.kill();
        let _ = child.wait();
        change(&mut random, &source, "s", &mut ours, 2)?;
        change(&mut random, &target, "t", &mut theirs, 2)?;
        if sync_until_done(&folder, &source, &target).is_err() {
            harm.reruns_not_done += 1;
        }
        meant.extend(ours);
        meant.extend(theirs);
        compare(&meant, &rows(&source)?, &mut harm);
        compare(&meant, &rows(&target)?, &mut harm);
        let _ = std::fs::remove_dir_all(&folder);
    }
    println!("{RUNS} killed runs: {harm:?}");
    match harm {
        Harm {
            deletes_undone: 0,
            rows_lost: 0,
            edits_lost: 0,
            reruns_not_done: 0,
        } => Ok(()),
        harm => Err(format!("{harm:?}")),
    }
}
