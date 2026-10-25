//! The NFR-SMUGGLR-005 measurement: five smugglr mesh processes on one
//! machine, their UDP traffic passed through a relay that drops 10% of
//! packets, the sunset set to 600 s, one node stopped for 400 s and
//! restarted, with inserts, edits and deletes on every node (each node
//! changes only rows it made); after quiet, every node's database is
//! compared against what the people meant.
//!
//! It runs in real time, about twelve minutes, so it runs only when asked:
//! `cargo test -p smugglr --test mesh_whole -- --ignored --nocapture`.

use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

const NODES: usize = 5;

/// The node that sleeps.
const SLEEPER: usize = 4;

/// The sunset: the graveyard window, in seconds.
const SUNSET: u64 = 600;

/// How long the sleeper is stopped: two thirds of the sunset.
const ASLEEP: Duration = Duration::from_secs(400);

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
}

/// Rows by id: what a node holds, or what the people meant.
type Rows = BTreeMap<String, String>;

fn sql(path: &Path, statement: &str) -> Result<(), String> {
    Connection::open(path)
        .and_then(|db| {
            db.busy_timeout(Duration::from_secs(10))?;
            db.execute_batch(statement)
        })
        .map_err(|err| format!("{}: {err}", path.display()))
}

fn rows(path: &Path) -> Result<Rows, String> {
    let db = Connection::open(path).map_err(|err| err.to_string())?;
    db.busy_timeout(Duration::from_secs(10))
        .map_err(|err| err.to_string())?;
    let mut statement = db
        .prepare("SELECT id, body FROM notes")
        .map_err(|err| err.to_string())?;
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|err| err.to_string())?
        .collect::<Result<Rows, _>>()
        .map_err(|err| err.to_string())
}

/// Passes each datagram from one node's port to every other, dropping one
/// in ten at random; counts what it passed and dropped.
fn relay(
    ports: Vec<u16>,
    seed: u64,
    stop: Arc<AtomicBool>,
    counts: Arc<[AtomicU64; 2]>,
) -> Result<u16, String> {
    let socket = UdpSocket::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    socket
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|err| err.to_string())?;
    let port = socket.local_addr().map_err(|err| err.to_string())?.port();
    thread::spawn(move || {
        let mut random = Random(seed ^ 0x9e37_79b9_7f4a_7c15);
        let mut buffer = vec![0; 65_536];
        while !stop.load(Ordering::Relaxed) {
            let Ok((len, from)) = socket.recv_from(&mut buffer) else {
                continue;
            };
            for to in ports.iter().filter(|port| **port != from.port()) {
                if random.below(10) == 0 {
                    counts[1].fetch_add(1, Ordering::Relaxed);
                } else {
                    counts[0].fetch_add(1, Ordering::Relaxed);
                    let _ = socket.send_to(&buffer[..len], ("127.0.0.1", *to));
                }
            }
        }
    });
    Ok(port)
}

struct Node {
    path: PathBuf,
    config: PathBuf,
    port: u16,
    child: Option<Child>,
}

impl Node {
    fn start(&mut self, key: &Path, relay: u16) -> Result<(), String> {
        let child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .arg("mesh")
            .arg(&self.path)
            .arg("--key-file")
            .arg(key)
            .args(["--port", &self.port.to_string()])
            .args(["--send-to", &format!("127.0.0.1:{relay}")])
            .env("XDG_CONFIG_HOME", &self.config)
            .stdout(Stdio::null())
            .spawn()
            .map_err(|err| err.to_string())?;
        self.child = Some(child);
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// Every node's own rows, and the next id each node gives.
struct People {
    meant: Rows,
    next: [u64; NODES],
    random: Random,
}

impl People {
    /// Node `index` inserts, edits or deletes one of its own rows.
    fn act(&mut self, nodes: &[Node], index: usize) -> Result<(), String> {
        let own: Vec<String> = self
            .meant
            .keys()
            .filter(|id| id.starts_with(&format!("{index}-")))
            .cloned()
            .collect();
        let path = &nodes[index].path;
        let roll = self.random.below(10);
        if own.is_empty() || roll < 5 {
            let id = format!("{index}-{}", self.next[index]);
            self.next[index] += 1;
            let body = format!("made {}", self.random.next());
            sql(
                path,
                &format!("INSERT INTO notes VALUES ('{id}', '{body}')"),
            )?;
            self.meant.insert(id, body);
        } else {
            let at = usize::try_from(self.random.below(own.len() as u64)).unwrap_or(0);
            let id = &own[at];
            if roll < 8 {
                let body = format!("edited {}", self.random.next());
                sql(
                    path,
                    &format!("UPDATE notes SET body = '{body}' WHERE id = '{id}'"),
                )?;
                self.meant.insert(id.clone(), body);
            } else {
                sql(path, &format!("DELETE FROM notes WHERE id = '{id}'"))?;
                self.meant.remove(id);
            }
        }
        Ok(())
    }

    /// One round: every awake node acts once, then a pause.
    fn round(&mut self, nodes: &[Node], awake: &[usize]) -> Result<(), String> {
        for &index in awake {
            self.act(nodes, index)?;
        }
        sleep(Duration::from_millis(700));
        Ok(())
    }
}

#[test]
#[ignore = "the NFR-SMUGGLR-005 measurement: about twelve minutes in real time"]
#[expect(
    clippy::too_many_lines,
    reason = "the measurement reads top to bottom as it is written"
)]
fn five_nodes_stay_whole_through_loss_and_a_sleeper() -> Result<(), String> {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |since| {
            u64::try_from(since.as_nanos() & u128::from(u64::MAX)).unwrap_or(1) | 1
        });
    println!("seed {seed}");
    let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("mesh-whole-{seed}"));
    std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
    let key = folder.join("mesh.key");
    std::fs::write(&key, format!("{}\n", "07".repeat(32))).map_err(|err| err.to_string())?;
    let mut nodes = Vec::new();
    for index in 0..NODES {
        let path = folder.join(format!("node{index}.db"));
        sql(&path, "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)")?;
        let config = folder.join(format!("config{index}"));
        std::fs::create_dir_all(config.join("smugglr")).map_err(|err| err.to_string())?;
        std::fs::write(
            config.join("smugglr").join("config.toml"),
            format!("graveyard = {SUNSET}\n"),
        )
        .map_err(|err| err.to_string())?;
        let port = UdpSocket::bind("127.0.0.1:0")
            .and_then(|socket| socket.local_addr())
            .map_err(|err| err.to_string())?
            .port();
        nodes.push(Node {
            path,
            config,
            port,
            child: None,
        });
    }
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let relay = relay(
        nodes.iter().map(|node| node.port).collect(),
        seed,
        stop.clone(),
        counts.clone(),
    )?;
    for node in &mut nodes {
        node.start(&key, relay)?;
        sleep(Duration::from_millis(500));
    }
    sleep(Duration::from_secs(2));
    let mut people = People {
        meant: Rows::new(),
        next: [0; NODES],
        random: Random(seed),
    };
    let all: Vec<usize> = (0..NODES).collect();
    let others: Vec<usize> = (0..NODES).filter(|index| *index != SLEEPER).collect();
    let result = (|| {
        for _ in 0..20 {
            people.round(&nodes, &all)?;
        }
        nodes[SLEEPER].stop();
        let slept = Instant::now();
        println!("node {SLEEPER} stopped");
        for _ in 0..40 {
            people.round(&nodes, &others)?;
        }
        sleep(ASLEEP.saturating_sub(slept.elapsed()));
        nodes[SLEEPER].start(&key, relay)?;
        println!("node {SLEEPER} restarted after {:?}", slept.elapsed());
        sleep(Duration::from_secs(2));
        for _ in 0..10 {
            people.round(&nodes, &all)?;
        }
        // Quiet: wait for every node to match, as long as resends go on.
        let quiet = Instant::now();
        while quiet.elapsed() < Duration::from_secs(400) {
            let mut whole = true;
            for node in &nodes {
                whole &= rows(&node.path)? == people.meant;
            }
            if whole {
                println!("whole {:?} after quiet", quiet.elapsed());
                break;
            }
            sleep(Duration::from_secs(5));
        }
        let mut report = Vec::new();
        for (index, node) in nodes.iter().enumerate() {
            let held = rows(&node.path)?;
            let lost = people
                .meant
                .iter()
                .filter(|(id, _)| !held.contains_key(*id))
                .count();
            let undone = held
                .keys()
                .filter(|id| !people.meant.contains_key(*id))
                .count();
            let edits = people
                .meant
                .iter()
                .filter(|(id, body)| held.get(*id).is_some_and(|held| held != *body))
                .count();
            println!("node {index}: {lost} rows lost, {undone} deletes undone, {edits} edits lost");
            if lost + undone + edits > 0 {
                report.push(index);
            }
        }
        println!(
            "relay: {} passed, {} dropped; {} rows meant",
            counts[0].load(Ordering::Relaxed),
            counts[1].load(Ordering::Relaxed),
            people.meant.len()
        );
        if report.is_empty() {
            Ok(())
        } else {
            Err(format!("nodes {report:?} are not whole (seed {seed})"))
        }
    })();
    for node in &mut nodes {
        node.stop();
    }
    stop.store(true, Ordering::Relaxed);
    result
}
