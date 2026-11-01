//! Two real `smugglr mesh` processes apart, then rejoined (#626). Both change
//! the same rows while stopped, each winning some, and one is over its
//! backpressure line with rows it has not looked at yet: the conditions the
//! rejoins behind #613, #616 and #617 met only on live stores.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use rusqlite::Connection;

type Tried<T = ()> = Result<T, Box<dyn std::error::Error>>;

/// Rows the first node edits later, so its copy wins.
const FIRST_WINS: usize = 60;
/// Rows the second node edits later, so its copy wins.
const SECOND_WINS: usize = 60;
/// Rows only the first node has, queued behind the conflicts.
const NEW: usize = 2_000;
/// The first node's line, far under what it has to send.
const LOW_LINE: u64 = 60_000;
/// How long both nodes get to settle.
const SETTLE: Duration = Duration::from_secs(120);
/// The most changes the record may say were resent in the whole run. It is
/// well above what a settled run needs (hundreds) and far below a storm.
const MOST_RESENDS: u64 = 20_000;

struct Node {
    dir: PathBuf,
    db: PathBuf,
    log: PathBuf,
    child: Option<Child>,
}

impl Node {
    fn new(root: &Path, name: &str, port: u16, peer: u16, more: &str) -> Tried<Self> {
        let dir = root.join(name);
        fs::create_dir_all(dir.join("bin"))?;
        fs::create_dir_all(dir.join("config/smugglr"))?;
        let hostname = dir.join("bin/hostname");
        fs::write(&hostname, format!("#!/bin/sh\necho {name}\n"))?;
        fs::set_permissions(&hostname, fs::Permissions::from_mode(0o755))?;
        fs::write(
            dir.join("config/smugglr/config.toml"),
            format!(
                "port = {port}\npeers = [\"127.0.0.1:{peer}\"]\ngraveyard = 3600\nsteering = \"updated_at\"\n{more}"
            ),
        )?;
        let db = dir.join("node.db");
        let connection = Connection::open(&db)?;
        connection.execute_batch(
            "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT, updated_at TEXT);",
        )?;
        for row in 0..FIRST_WINS + SECOND_WINS {
            connection.execute(
                "INSERT INTO notes VALUES (?1, 'base', '2026-01-01')",
                [format!("r{row:05}")],
            )?;
        }
        Ok(Self {
            log: dir.join("log"),
            dir,
            db,
            child: None,
        })
    }

    fn start(&mut self, key: &Path) -> Tried {
        let path = format!(
            "{}:{}",
            self.dir.join("bin").display(),
            std::env::var("PATH")?
        );
        let log = File::options().create(true).append(true).open(&self.log)?;
        self.child = Some(
            Command::new(env!("CARGO_BIN_EXE_smugglr"))
                .args(["mesh"])
                .arg(&self.db)
                .arg("--key-file")
                .arg(key)
                .args(["--sync-mode", "last"])
                .env("PATH", path)
                .env("XDG_CONFIG_HOME", self.dir.join("config"))
                .stdout(Stdio::from(log.try_clone()?))
                .stderr(Stdio::from(log))
                .spawn()?,
        );
        Ok(())
    }

    fn stop(&mut self) -> Tried {
        if let Some(mut child) = self.child.take() {
            child.kill()?;
            child.wait()?;
        }
        Ok(())
    }

    fn rows(&self) -> Tried<BTreeMap<String, (String, String)>> {
        let connection = Connection::open(&self.db)?;
        let mut rows = connection.prepare("SELECT id, body, updated_at FROM notes")?;
        let rows = rows.query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// How many changes wait in the outbox, by its segment files.
    fn outbox_segments(&self) -> Tried<usize> {
        let mesh = self.dir.join("config/smugglr/mesh");
        let mut segments = 0;
        for folder in fs::read_dir(mesh)? {
            let outbox = folder?.path().join("outbox");
            if outbox.is_dir() {
                for file in fs::read_dir(outbox)? {
                    let name = file?.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with('0') && name.ends_with(".jsonl") {
                        segments += 1;
                    }
                }
            }
        }
        Ok(segments)
    }

    fn printed(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

fn free_port() -> Tried<u16> {
    Ok(std::net::UdpSocket::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// Sets `rows` of a node to `body` at `updated_at`.
fn edit(db: &Path, rows: std::ops::Range<usize>, body: &str, updated_at: &str) -> Tried {
    let mut connection = Connection::open(db)?;
    let edit = connection.transaction()?;
    for row in rows {
        edit.execute(
            "UPDATE notes SET body = ?1, updated_at = ?2 WHERE id = ?3",
            [body, updated_at, &format!("r{row:05}")],
        )?;
    }
    edit.commit()?;
    Ok(())
}

/// What the record says was resent, added up.
fn resends(printed: &str) -> u64 {
    printed
        .lines()
        .filter_map(|line| line.strip_prefix("smugglr mesh: resent "))
        .filter_map(|rest| rest.split(' ').next()?.parse::<u64>().ok())
        .sum()
}

#[test]
fn two_nodes_apart_settle_on_one_row_each_and_drain() -> Tried {
    let root = std::env::temp_dir().join(format!("smugglr-rejoin-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root)?;
    let (first_port, second_port) = (free_port()?, free_port()?);
    let more = format!("backpressure = {LOW_LINE}\n");
    let mut first = Node::new(&root, "first", first_port, second_port, &more)?;
    let mut second = Node::new(&root, "second", second_port, first_port, "")?;
    let key = root.join("mesh.key");
    let made = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["mesh", "keygen", "--key-file"])
        .arg(&key)
        .stdout(Stdio::null())
        .status()?;
    assert!(made.success());
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600))?;

    // Each node notes its rows and sends nothing, then both stop.
    first.start(&key)?;
    second.start(&key)?;
    sleep(Duration::from_secs(4));
    first.stop()?;
    second.stop()?;

    // Apart: both edit the same rows, and only the first adds new ones.
    edit(&first.db, 0..FIRST_WINS, "first", "2026-03-01")?;
    edit(&second.db, 0..FIRST_WINS, "second", "2026-02-01")?;
    let both = FIRST_WINS..FIRST_WINS + SECOND_WINS;
    edit(&first.db, both.clone(), "first", "2026-02-01")?;
    edit(&second.db, both, "second", "2026-03-01")?;
    let mut connection = Connection::open(&first.db)?;
    let add = connection.transaction()?;
    for row in 0..NEW {
        add.execute(
            "INSERT INTO notes VALUES (?1, 'new', '2026-04-01')",
            [format!("n{row:05}")],
        )?;
    }
    add.commit()?;

    first.start(&key)?;
    second.start(&key)?;
    let started = Instant::now();
    let settled = loop {
        if first.rows()? == second.rows()?
            && first.outbox_segments()? == 0
            && second.outbox_segments()? == 0
        {
            break true;
        }
        if started.elapsed() > SETTLE {
            break false;
        }
        sleep(Duration::from_secs(1));
    };
    let (first_printed, second_printed) = (first.printed(), second.printed());
    let (first_rows, second_rows) = (first.rows()?, second.rows()?);
    let differ = first_rows
        .iter()
        .filter(|(id, row)| second_rows.get(*id) != Some(*row))
        .count();
    assert!(
        settled,
        "not settled within {SETTLE:?}: {differ} rows differ, outboxes {} and {}\nfirst:\n{first_printed}\nsecond:\n{second_printed}",
        first.outbox_segments()?,
        second.outbox_segments()?,
    );
    assert_eq!(first_rows.len(), FIRST_WINS + SECOND_WINS + NEW);
    for (id, (body, _)) in &second_rows {
        let row: usize = id[1..].parse()?;
        let wins = if id.starts_with('n') {
            "new"
        } else if row < FIRST_WINS {
            "first"
        } else {
            "second"
        };
        assert_eq!(body, wins, "row {id}");
    }
    let all = format!("{first_printed}{second_printed}");
    assert!(!all.contains("dropped a change"), "{all}");
    assert!(!all.contains("the row moved here"), "{all}");
    let resent = resends(&first_printed) + resends(&second_printed);
    assert!(resent <= MOST_RESENDS, "{resent} resends\n{all}");
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

/// Two nodes that have noted their rows and heard each other, the second's
/// config carrying `second_more`.
fn two_running(name: &str, second_more: &str) -> Tried<(PathBuf, PathBuf, Node, Node)> {
    let root = std::env::temp_dir().join(format!("smugglr-rejoin-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root)?;
    let (first_port, second_port) = (free_port()?, free_port()?);
    let mut first = Node::new(&root, "first", first_port, second_port, "")?;
    let mut second = Node::new(&root, "second", second_port, first_port, second_more)?;
    let key = root.join("mesh.key");
    let made = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["mesh", "keygen", "--key-file"])
        .arg(&key)
        .stdout(Stdio::null())
        .status()?;
    assert!(made.success());
    fs::set_permissions(&key, fs::Permissions::from_mode(0o600))?;
    first.start(&key)?;
    second.start(&key)?;
    sleep(Duration::from_secs(3));
    Ok((root, key, first, second))
}

/// Waits up to `within` for `done`.
fn soon(within: Duration, mut done: impl FnMut() -> Tried<bool>) -> Tried<bool> {
    let started = Instant::now();
    while started.elapsed() < within {
        if done()? {
            return Ok(true);
        }
        sleep(Duration::from_millis(200));
    }
    Ok(false)
}

#[test]
fn a_node_that_wakes_a_second_time_is_answered_at_once_too() -> Tried {
    let (root, key, mut first, second) = two_running("wakes", "")?;
    for wake in 0..2 {
        first.stop()?;
        // Long enough that its peer has stopped hearing it.
        sleep(Duration::from_secs(12));
        let id = format!("w{wake}");
        Connection::open(&second.db)?.execute(
            "INSERT INTO notes VALUES (?1, 'while asleep', '2026-05-01')",
            [&id],
        )?;
        sleep(Duration::from_secs(1));
        first.start(&key)?;
        // Past the 1 s listen; well inside the 10 s of a resend to a peer
        // not acknowledging, which is what a wazzup not heard would wait for.
        let arrived = soon(Duration::from_secs(6), || {
            Ok(first.rows()?.contains_key(&id))
        })?;
        assert!(arrived, "wake {wake}: {id} did not arrive at once");
    }
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn a_peer_owing_more_than_the_wire_says_so_and_how_to_catch_up() -> Tried {
    let (root, key, mut first, second) = two_running("too-much", "mesh_wire = 1000\n")?;
    first.stop()?;
    let mut connection = Connection::open(&second.db)?;
    let add = connection.transaction()?;
    for row in 0..200 {
        add.execute(
            "INSERT INTO notes VALUES (?1, 'too many for the wire', '2026-05-01')",
            [format!("m{row:05}")],
        )?;
    }
    add.commit()?;
    sleep(Duration::from_secs(2));
    first.start(&key)?;
    let said = soon(Duration::from_secs(10), || {
        Ok(first
            .printed()
            .contains("says there is too much for the wire"))
    })?;
    assert!(said, "{}", first.printed());
    assert!(first.printed().contains("smugglr retrieve") || first.printed().contains("retrieve "));
    let _ = fs::remove_dir_all(&root);
    Ok(())
}
