//! Steps for `smugglr mesh` and `smugglr sync --sync-type mesh`: two
//! smugglr processes on SQLite files in the scenario's folder, each on its
//! own port on this machine, a relay passing each one's datagrams to the
//! other.

use std::net::UdpSocket;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use rusqlite::Connection;

use crate::setup::World;

/// The two nodes' processes and the relay between them; dropping it stops
/// them.
#[derive(Debug, Default)]
pub(crate) struct Nodes {
    children: Vec<Child>,
    paths: Vec<String>,
    stop: Arc<AtomicBool>,
}

impl Drop for Nodes {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// A scenario's mesh nodes.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Front {
    nodes: Slot<Arc<Nodes>>,
}

#[fixture]
pub(crate) fn front() -> Front {
    Front::default()
}

fn free_port() -> Result<u16, String> {
    UdpSocket::bind("127.0.0.1:0")
        .and_then(|socket| socket.local_addr())
        .map(|addr| addr.port())
        .map_err(|err| err.to_string())
}

/// Passes each datagram from one of `ports` to the others, until `stop`.
fn relay(ports: [u16; 2], stop: Arc<AtomicBool>) -> Result<u16, String> {
    let socket = UdpSocket::bind("127.0.0.1:0").map_err(|err| err.to_string())?;
    socket
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|err| err.to_string())?;
    let port = socket.local_addr().map_err(|err| err.to_string())?.port();
    thread::spawn(move || {
        let mut buffer = vec![0; 65_536];
        while !stop.load(Ordering::Relaxed) {
            if let Ok((len, from)) = socket.recv_from(&mut buffer) {
                for to in ports.into_iter().filter(|port| *port != from.port()) {
                    let _ = socket.send_to(&buffer[..len], ("127.0.0.1", to));
                }
            }
        }
    });
    Ok(port)
}

#[given(
    "one node started with smugglr mesh and another with smugglr sync --sync-type mesh, with the same key file"
)]
fn two_front_ends(world: &World, front: &Front) -> Result<(), String> {
    let folder = world.folder()?;
    let key = folder.join("mesh.key");
    std::fs::write(&key, format!("{}\n", "07".repeat(32))).map_err(|err| err.to_string())?;
    let ports = [free_port()?, free_port()?];
    let mut nodes = Nodes::default();
    let relay = relay(ports, nodes.stop.clone())?;
    let commands: [&[&str]; 2] = [&["mesh"], &["sync"]];
    for (index, command) in commands.iter().enumerate() {
        let path = folder.join(format!("node{index}.db"));
        Connection::open(&path)
            .and_then(|db| db.execute_batch("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)"))
            .map_err(|err| err.to_string())?;
        let mut args: Vec<String> = command.iter().map(|arg| (*arg).to_owned()).collect();
        args.push(path.display().to_string());
        if index == 1 {
            args.extend(["--sync-type".to_owned(), "mesh".to_owned()]);
        }
        args.extend([
            "--key-file".to_owned(),
            key.display().to_string(),
            "--port".to_owned(),
            ports[index].to_string(),
            "--send-to".to_owned(),
            format!("127.0.0.1:{relay}"),
        ]);
        let child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
            .args(&args)
            .env("XDG_CONFIG_HOME", folder.join(format!("config{index}")))
            .stdout(Stdio::null())
            .spawn()
            .map_err(|err| format!("failed to start smugglr: {err}"))?;
        nodes.children.push(child);
        nodes.paths.push(path.display().to_string());
        // The first node listens before the second says it is here.
        sleep(Duration::from_millis(500));
    }
    // Both have looked at their database once before a row is written.
    sleep(Duration::from_secs(1));
    front.nodes.set(Arc::new(nodes));
    Ok(())
}

fn path(front: &Front, index: usize) -> Result<String, String> {
    front
        .nodes
        .get()
        .and_then(|nodes| nodes.paths.get(index).cloned())
        .ok_or_else(|| format!("no node {index}"))
}

#[when("a row is written on the first node's database")]
fn a_row_on_the_first(front: &Front) -> Result<(), String> {
    Connection::open(path(front, 0)?)
        .and_then(|db| {
            db.busy_timeout(Duration::from_secs(5))?;
            db.execute_batch("INSERT INTO notes VALUES ('n9', 'across')")
        })
        .map_err(|err| err.to_string())
}

#[then("the second node holds that row")]
fn the_second_holds_it(front: &Front) -> Result<(), String> {
    let second = path(front, 1)?;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(10) {
        let body: Option<String> = Connection::open(&second)
            .and_then(|db| {
                db.busy_timeout(Duration::from_secs(5))?;
                db.query_row("SELECT body FROM notes WHERE id = 'n9'", [], |row| {
                    row.get(0)
                })
            })
            .ok();
        if body.as_deref() == Some("across") {
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    Err("the second node does not hold the row".to_owned())
}
