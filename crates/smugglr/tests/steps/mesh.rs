//! Steps for `smugglr mesh` as a command: smugglr processes on SQLite files
//! in the scenario's folder, each with its own config directory and its own
//! machine name (a `hostname` first on its `PATH`), broadcasting on the
//! port its config names.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::net::UdpSocket;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, sleep};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use rstest::fixture;
use rstest_bdd::Slot;
use rstest_bdd_macros::{ScenarioState, given, then, when};
use rusqlite::Connection;
use serde_json::{Value, json};
use smugglr_core::change::{Change, Op};
use smugglr_core::envelope::{Envelope, VERSION};
use smugglr_core::hex::hex;
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::{Column, Stamp};

use crate::setup::World;

/// The table every node's database holds.
const NOTES: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)";

/// How long a step waits for a change sent at once.
const SOON: Duration = Duration::from_secs(10);

/// A key no node of a scenario holds.
const UNKNOWN: [u8; 32] = [9; 32];

/// How a node is started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FrontEnd {
    /// `smugglr mesh <place>`.
    Mesh,
    /// `smugglr sync <place> --sync-type mesh`.
    Sync,
    /// `smugglr mesh <place> --sync-mode stop`.
    MeshStop,
}

/// One running smugglr process.
#[derive(Debug)]
struct Node {
    path: PathBuf,
    out: PathBuf,
    err: PathBuf,
    port: u16,
    child: Child,
    /// How it was started, to start it again.
    command: Command,
}

impl Node {
    /// What the node has printed to its output and its errors.
    fn printed(&self) -> String {
        let read = |path: &Path| std::fs::read_to_string(path).unwrap_or_default();
        format!("{}{}", read(&self.out), read(&self.err))
    }
}

/// A scenario's nodes; dropping them stops them.
#[derive(Debug, Default)]
struct Nodes(Mutex<Vec<Node>>);

impl Drop for Nodes {
    fn drop(&mut self) {
        for node in self.0.get_mut().unwrap_or_else(PoisonError::into_inner) {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

/// A scenario's mesh nodes and key files.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Front {
    nodes: Slot<Arc<Nodes>>,
    /// The key ids keygen printed, in the order it wrote them.
    key_ids: Slot<Vec<String>>,
    /// The output of a node that did not start.
    refused: Slot<std::process::Output>,
    /// A key file a scenario names.
    key: Slot<PathBuf>,
    /// The relay a scenario's nodes talk through.
    relay: Slot<Arc<Relay>>,
    /// The scenario's turn on the machine, for one that floods it.
    turn: Slot<Arc<Turn>>,
}

#[fixture]
pub(crate) fn front() -> Front {
    Front::default()
}

impl Front {
    fn nodes(&self) -> Arc<Nodes> {
        if let Some(nodes) = self.nodes.get() {
            return nodes;
        }
        let nodes = Arc::new(Nodes::default());
        self.nodes.set(nodes.clone());
        nodes
    }

    /// Starts node `name` on `port` (`None`: no port setting) with the key
    /// file `key`, and waits for its first look at its rows.
    fn start(
        &self,
        world: &World,
        name: &str,
        key: &Path,
        port: Option<u16>,
        front_end: FrontEnd,
    ) -> Result<usize, String> {
        self.start_with(world, name, key, port, front_end, "")
    }

    /// Starts a node as [`Front::start`] does, `more` added to its config.
    fn start_with(
        &self,
        world: &World,
        name: &str,
        key: &Path,
        port: Option<u16>,
        front_end: FrontEnd,
        more: &str,
    ) -> Result<usize, String> {
        let folder = world.folder()?.join(name);
        std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
        let path = folder.join("node.db");
        Connection::open(&path)
            .and_then(|db| db.execute_batch(NOTES))
            .map_err(|err| err.to_string())?;
        let config = folder.join("config").join("smugglr");
        std::fs::create_dir_all(&config).map_err(|err| err.to_string())?;
        let settings = port
            .map(|port| format!("port = {port}\n"))
            .unwrap_or_default()
            + more;
        if !settings.is_empty() {
            std::fs::write(config.join("config.toml"), settings).map_err(|err| err.to_string())?;
        }
        let bin = folder.join("bin");
        std::fs::create_dir_all(&bin).map_err(|err| err.to_string())?;
        machine_named(&bin, name)?;
        let place = path.display().to_string();
        let key_file = key.display().to_string();
        let args: Vec<&str> = match front_end {
            FrontEnd::Mesh => vec!["mesh", &place, "--key-file", &key_file],
            FrontEnd::MeshStop => vec![
                "mesh",
                &place,
                "--key-file",
                &key_file,
                "--sync-mode",
                "stop",
            ],
            FrontEnd::Sync => vec![
                "sync",
                &place,
                "--sync-type",
                "mesh",
                "--key-file",
                &key_file,
            ],
        };
        let (out, err) = (folder.join("out.txt"), folder.join("err.txt"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_smugglr"));
        command
            .args(&args)
            .env("XDG_CONFIG_HOME", folder.join("config"))
            .env("PATH", search_path(&bin))
            // Windows names the machine in COMPUTERNAME, not `hostname`.
            .env("COMPUTERNAME", name);
        let child = spawn(&mut command, &out, &err)?;
        let nodes = self.nodes();
        let mut nodes = nodes.0.lock().unwrap_or_else(PoisonError::into_inner);
        nodes.push(Node {
            path,
            out,
            err,
            port: port.unwrap_or(31337),
            child,
            command,
        });
        let index = nodes.len() - 1;
        drop(nodes);
        // A node's first look notes every row as it is and sends nothing;
        // it is done once the node keeps what it saw.
        wait(SOON, || {
            std::fs::read_dir(&config).is_ok_and(|entries| {
                entries
                    .flatten()
                    .any(|entry| entry.file_name() != "config.toml")
            })
        })
        .map_err(|()| format!("{name} did not look at its rows"))?;
        Ok(index)
    }

    /// Stops node `index`.
    fn stop(&self, index: usize) -> Result<(), String> {
        let nodes = self.nodes();
        let mut nodes = nodes.0.lock().unwrap_or_else(PoisonError::into_inner);
        let node = nodes.get_mut(index).ok_or(format!("no node {index}"))?;
        let _ = node.child.kill();
        let _ = node.child.wait();
        Ok(())
    }

    /// Starts node `index` again, stopped, as it was started.
    fn restart(&self, index: usize) -> Result<(), String> {
        let nodes = self.nodes();
        let mut nodes = nodes.0.lock().unwrap_or_else(PoisonError::into_inner);
        let node = nodes.get_mut(index).ok_or(format!("no node {index}"))?;
        node.child = spawn(&mut node.command, &node.out, &node.err)?;
        Ok(())
    }

    fn with<T>(&self, index: usize, read: impl FnOnce(&Node) -> T) -> Result<T, String> {
        let nodes = self.nodes();
        let nodes = nodes.0.lock().unwrap_or_else(PoisonError::into_inner);
        nodes.get(index).map(read).ok_or(format!("no node {index}"))
    }

    fn sql(&self, index: usize, sql: &str) -> Result<(), String> {
        let path = self.with(index, |node| node.path.clone())?;
        Connection::open(path)
            .and_then(|db| {
                db.busy_timeout(Duration::from_secs(5))?;
                db.execute_batch(sql)
            })
            .map_err(|err| err.to_string())
    }

    /// Waits for node `index` to hold note `id` with `body`.
    fn holds(&self, index: usize, id: &str, body: &str) -> Result<(), String> {
        self.holds_within(index, id, body, SOON)
    }

    /// Waits up to `within` for node `index` to hold note `id` with `body`.
    fn holds_within(
        &self,
        index: usize,
        id: &str,
        body: &str,
        within: Duration,
    ) -> Result<(), String> {
        let path = self.with(index, |node| node.path.clone())?;
        let read = || {
            Connection::open(&path)
                .and_then(|db| {
                    db.busy_timeout(Duration::from_secs(5))?;
                    db.query_row("SELECT body FROM notes WHERE id = ?1", [id], |row| {
                        row.get::<_, String>(0)
                    })
                })
                .ok()
        };
        wait(within, || read().as_deref() == Some(body))
            .map_err(|()| format!("node {index} holds {:?} for {id}, not {body:?}", read()))
    }

    /// Waits for node `index` to print `text`.
    fn prints(&self, index: usize, text: &str) -> Result<(), String> {
        wait(SOON, || {
            self.with(index, |node| node.printed().contains(text))
                .unwrap_or(false)
        })
        .map_err(|()| {
            format!(
                "node {index} did not print {text:?}; it printed {:?}",
                self.with(index, Node::printed).unwrap_or_default()
            )
        })
    }
}

/// Starts `nodes` (each its name, key file and front end), each on its own
/// port and naming the others' in its peers setting, `more` added to each
/// config. They talk unicast on loopback, so what a scenario shows does not
/// hang on the system delivering broadcasts.
fn start_named(
    world: &World,
    front: &Front,
    nodes: &[(&str, &Path, FrontEnd)],
    more: &str,
) -> Result<(), String> {
    let ports = nodes
        .iter()
        .map(|_| free_port())
        .collect::<Result<Vec<u16>, String>>()?;
    for (at, (name, key, front_end)) in nodes.iter().enumerate() {
        let peers: Vec<String> = ports
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != at)
            .map(|(_, port)| format!("127.0.0.1:{port}"))
            .collect();
        let config = format!("peers = {peers:?}\n{more}");
        front.start_with(world, name, key, Some(ports[at]), *front_end, &config)?;
    }
    Ok(())
}

/// Runs `command`, its output and errors written to `out` and `err`.
fn spawn(command: &mut Command, out: &Path, err: &Path) -> Result<Child, String> {
    let file = |path: &Path| File::create(path).map_err(|err| err.to_string());
    command
        .stdout(Stdio::from(file(out)?))
        .stderr(Stdio::from(file(err)?))
        .spawn()
        .map_err(|err| format!("failed to start smugglr: {err}"))
}

/// Puts a `hostname` in `bin` that names this machine `name`.
fn machine_named(bin: &Path, name: &str) -> Result<(), String> {
    let script = bin.join("hostname");
    std::fs::write(&script, format!("#!/bin/sh\necho {name}\n")).map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

/// `bin`, then the search path this test runs with.
fn search_path(bin: &Path) -> std::ffi::OsString {
    let rest = std::env::var_os("PATH").unwrap_or_default();
    std::env::join_paths(std::iter::once(bin.to_path_buf()).chain(std::env::split_paths(&rest)))
        .unwrap_or(rest)
}

/// A UDP port no process holds now.
fn free_port() -> Result<u16, String> {
    UdpSocket::bind("0.0.0.0:0")
        .and_then(|socket| socket.local_addr())
        .map(|addr| addr.port())
        .map_err(|err| err.to_string())
}

/// Runs `smugglr mesh keygen --key-file <path>` and returns the key id it
/// printed.
fn keygen(path: &Path) -> Result<String, String> {
    let output = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["mesh", "keygen", "--key-file"])
        .arg(path)
        .output()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    let printed = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() {
        return Err(format!(
            "keygen failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    printed
        .split_whitespace()
        .skip_while(|word| *word != "key")
        .nth(1)
        .map(str::to_owned)
        .ok_or_else(|| format!("keygen printed no key id: {printed:?}"))
}

/// A key file made by keygen in the scenario's folder; returns its path.
fn key_file(world: &World, front: &Front, name: &str) -> Result<PathBuf, String> {
    let path = world.folder()?.join(name);
    let id = keygen(&path)?;
    let mut ids = front.key_ids.get().unwrap_or_default();
    ids.push(id);
    front.key_ids.set(ids);
    Ok(path)
}

/// Waits up to `within` for `done` to hold.
fn wait(within: Duration, mut done: impl FnMut() -> bool) -> Result<(), ()> {
    let started = Instant::now();
    while started.elapsed() < within {
        if done() {
            return Ok(());
        }
        sleep(Duration::from_millis(100));
    }
    if done() { Ok(()) } else { Err(()) }
}

#[given(
    "one node started with smugglr mesh and another with smugglr sync --sync-type mesh, with the same key file"
)]
fn two_front_ends(world: &World, front: &Front) -> Result<(), String> {
    let key = key_file(world, front, "mesh.key")?;
    start_named(
        world,
        front,
        &[
            ("mesh-node", &key, FrontEnd::Mesh),
            ("sync-node", &key, FrontEnd::Sync),
        ],
        "",
    )
}

#[when("a row is written on the first node's database")]
fn a_row_on_the_first(front: &Front) -> Result<(), String> {
    front.sql(0, "INSERT INTO notes VALUES ('n9', 'across')")
}

#[then("the second node holds that row")]
fn the_second_holds_it(front: &Front) -> Result<(), String> {
    front.holds(1, "n9", "across")
}

#[given("a node running smugglr mesh on a machine named puck")]
fn a_node_on_puck(world: &World, front: &Front) -> Result<(), String> {
    let key = key_file(world, front, "mesh.key")?;
    start_named(
        world,
        front,
        &[
            ("puck", &key, FrontEnd::Mesh),
            ("rzr", &key, FrontEnd::Mesh),
        ],
        "",
    )
}

#[when("a row is written on puck's database")]
fn a_row_on_puck(front: &Front) -> Result<(), String> {
    front.sql(0, "INSERT INTO notes VALUES ('n9', 'from puck')")
}

#[then("the change the other nodes receive names puck as its node")]
fn the_change_names_puck(front: &Front) -> Result<(), String> {
    front.holds(1, "n9", "from puck")?;
    front.prints(1, "to notes from puck,")
}

#[given("one node with no port setting and one node with port set to 40000")]
fn two_ports(world: &World, front: &Front) -> Result<(), String> {
    key_file(world, front, "mesh.key").map(|key| front.key.set(key))
}

#[when("each node starts smugglr mesh")]
fn each_starts(world: &World, front: &Front) -> Result<(), String> {
    let key = front.key.get().ok_or("no key file")?;
    front.start(world, "unset", &key, None, FrontEnd::Mesh)?;
    front.start(world, "set", &key, Some(40000), FrontEnd::Mesh)?;
    Ok(())
}

/// A partner on `port` exchanges a row each way with node `index`.
fn exchanges_on(world: &World, front: &Front, index: usize, port: u16) -> Result<(), String> {
    let key = front.key.get().ok_or("no key file")?;
    // The partner, on a port of its own, names the node at `port` in its
    // peers setting: it reaches the node only there, and hears from it only
    // if the node sends from there.
    let partner = front.start_with(
        world,
        &format!("on-{port}"),
        &key,
        Some(free_port()?),
        FrontEnd::Mesh,
        &format!("peers = [\"127.0.0.1:{port}\"]\n"),
    )?;
    let (to, back) = (format!("to-{port}"), format!("back-{port}"));
    front.sql(index, &format!("INSERT INTO notes VALUES ('{to}', 'sent')"))?;
    front.holds_within(partner, &to, "sent", Duration::from_secs(30))?;
    front.sql(
        partner,
        &format!("INSERT INTO notes VALUES ('{back}', 'heard')"),
    )?;
    front.holds_within(index, &back, "heard", Duration::from_secs(30))
}

#[then("the first sends and listens on UDP port 31337")]
fn the_first_on_31337(world: &World, front: &Front) -> Result<(), String> {
    if front.with(0, |node| node.port)? != 31337 {
        return Err("the first node was not started without a port".to_owned());
    }
    exchanges_on(world, front, 0, 31337)
}

#[then("the second sends and listens on UDP port 40000")]
fn the_second_on_40000(world: &World, front: &Front) -> Result<(), String> {
    exchanges_on(world, front, 1, 40000)
}

#[given("a key file readable by its group")]
fn a_group_readable_key(world: &World, front: &Front) -> Result<(), String> {
    // Windows has no group or other read bits for smugglr to check: the
    // refusal is built for the systems that do (unix), and is a gap for
    // Windows, named in #520.
    if cfg!(windows) {
        return Ok(());
    }
    let key = key_file(world, front, "mesh.key")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o640))
            .map_err(|err| err.to_string())?;
    }
    front.key.set(key);
    Ok(())
}

#[when("a node starts smugglr mesh with it")]
fn a_node_starts_with_it(world: &World, front: &Front) -> Result<(), String> {
    // Windows has no group or other read bits for smugglr to check: the
    // refusal is built for the systems that do (unix), and is a gap for
    // Windows, named in #520.
    if cfg!(windows) {
        return Ok(());
    }
    let key = front.key.get().ok_or("no key file")?;
    let place = world.place("node.db", NOTES)?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_smugglr"))
        .args(["mesh", &place, "--key-file"])
        .arg(&key)
        .env("XDG_CONFIG_HOME", world.xdg()?)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start smugglr: {err}"))?;
    // A node that started runs until stopped.
    if wait(SOON, || child.try_wait().ok().flatten().is_some()).is_err() {
        let _ = child.kill();
    }
    let output = child.wait_with_output().map_err(|err| err.to_string())?;
    front.refused.set(output);
    Ok(())
}

#[then("the node does not start")]
fn the_node_does_not_start(front: &Front) -> Result<(), String> {
    // Windows has no group or other read bits for smugglr to check: the
    // refusal is built for the systems that do (unix), and is a gap for
    // Windows, named in #520.
    if cfg!(windows) {
        return Ok(());
    }
    let output = front.refused.get().ok_or("no node was started")?;
    if output.status.success() || output.status.code().is_none() {
        return Err(format!("the node ran: {}", output.status));
    }
    Ok(())
}

#[then("it says chmod 600 on that key file fixes it")]
fn it_says_chmod(front: &Front) -> Result<(), String> {
    // Windows has no group or other read bits for smugglr to check: the
    // refusal is built for the systems that do (unix), and is a gap for
    // Windows, named in #520.
    if cfg!(windows) {
        return Ok(());
    }
    let output = front.refused.get().ok_or("no node was started")?;
    let key = front.key.get().ok_or("no key file")?;
    let said = String::from_utf8_lossy(&output.stderr);
    let fix = format!("chmod 600 {}", key.display());
    if said.contains(&fix) {
        Ok(())
    } else {
        Err(format!("it said {said:?}, not {fix:?}"))
    }
}

#[given("a key file written by smugglr mesh keygen and shared by two nodes")]
fn a_shared_keygen_file(world: &World, front: &Front) -> Result<(), String> {
    let first = key_file(world, front, "first.key")?;
    let second = world.folder()?.join("second.key");
    std::fs::copy(&first, &second).map_err(|err| err.to_string())?;
    front.key.set(first);
    Ok(())
}

#[when("smugglr mesh keygen appends a second key on the first node only")]
fn keygen_appends(world: &World, front: &Front) -> Result<(), String> {
    let first = front.key.get().ok_or("no key file")?;
    let newer = keygen(&first)?;
    let mut ids = front.key_ids.get().unwrap_or_default();
    ids.push(newer);
    front.key_ids.set(ids);
    let second = world.folder()?.join("second.key");
    // A third node holding both keys, to hear what the first signs.
    start_named(
        world,
        front,
        &[
            ("first", &first, FrontEnd::Mesh),
            ("second", &second, FrontEnd::Mesh),
            ("witness", &first, FrontEnd::Mesh),
        ],
        "",
    )
}

#[then("the first node's changes are signed with the newer key")]
fn the_first_signs_newer(front: &Front) -> Result<(), String> {
    let ids = front.key_ids.get().ok_or("keygen wrote no key")?;
    let newer = ids.last().ok_or("keygen wrote no key")?;
    front.sql(0, "INSERT INTO notes VALUES ('n1', 'newer')")?;
    front.holds(2, "n1", "newer")?;
    front.prints(2, &format!("from first, key {newer}"))
}

#[then("the first node still accepts a change the second node signs with the older key")]
fn the_first_accepts_older(front: &Front) -> Result<(), String> {
    let ids = front.key_ids.get().ok_or("keygen wrote no key")?;
    let older = ids.first().ok_or("keygen wrote no key")?;
    front.sql(1, "INSERT INTO notes VALUES ('n2', 'older')")?;
    front.holds(0, "n2", "older")?;
    front.prints(0, &format!("from second, key {older}"))
}

#[given("two nodes running smugglr mesh with one key file")]
fn two_nodes_one_key(world: &World, front: &Front) -> Result<(), String> {
    let key = key_file(world, front, "mesh.key")?;
    start_named(
        world,
        front,
        &[
            ("left", &key, FrontEnd::Mesh),
            ("right", &key, FrontEnd::Mesh),
        ],
        "",
    )?;
    front.key.set(key);
    Ok(())
}

/// A change to note `id` signed with `key`, sealed with the mesh's shared
/// key `shared`, as the wire carries it.
fn forged(id: &str, signed: &[u8; 32], shared: &[u8; 32]) -> Result<Vec<u8>, String> {
    let values = vec![format!("'{id}'"), "'forged'".to_owned()];
    let refs: Vec<&str> = values.iter().map(String::as_str).collect();
    let columns: Vec<Column> = ["id", "body"]
        .iter()
        .map(|name| Column {
            name: (*name).to_owned(),
            kind: "TEXT".to_owned(),
        })
        .collect();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let mut envelope = Envelope {
        v: VERSION,
        node: "stranger".to_owned(),
        change: Change {
            op: Op::Up,
            table: "notes".to_owned(),
            key: vec![format!("'{id}'")],
            from: None,
            to: Some(row_hash(&refs)),
            values,
        },
        targets: Vec::new(),
        sunset: now + 600,
        txn: None,
        key_id: smugglr_mesh::key_id(signed),
        stamp: Stamp::of(&columns),
        signature: [0; 32],
    };
    envelope.sign(&blake3::derive_key(
        "smugglr mesh 0.6 envelope signature",
        signed,
    ));
    let message = json!({"t": "change", "sender": "stranger", "envelope": envelope.to_json()});
    let seal = blake3::derive_key("smugglr mesh 0.6 datagram seal", shared);
    let cipher = XChaCha20Poly1305::new((&seal).into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let sealed = cipher
        .encrypt(&nonce, message.to_string().as_bytes())
        .map_err(|err| err.to_string())?;
    let mut datagram = nonce.to_vec();
    datagram.extend(sealed);
    Ok(datagram)
}

/// The keys a key file holds, as its lines give them.
fn key_lines(path: &Path) -> Result<Vec<String>, String> {
    Ok(std::fs::read_to_string(path)
        .map_err(|err| err.to_string())?
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_owned)
        .collect())
}

#[when("a row is written and a change signed with an unknown key arrives")]
fn a_row_and_a_stranger(front: &Front) -> Result<(), String> {
    front.sql(0, "INSERT INTO notes VALUES ('n1', 'written')")?;
    front.holds(1, "n1", "written")?;
    let key = front.key.get().ok_or("no key file")?;
    let line = key_lines(&key)?.pop().ok_or("the key file holds no key")?;
    let shared = decode(&line)?;
    let port = front.with(1, |node| node.port)?;
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|err| err.to_string())?;
    socket.set_broadcast(true).map_err(|err| err.to_string())?;
    socket
        .send_to(&forged("n2", &UNKNOWN, &shared)?, ("127.0.0.1", port))
        .map_err(|err| err.to_string())?;
    front.prints(1, "refused a change")
}

#[then("neither node's output or errors contain the key")]
fn no_key_in_output(front: &Front) -> Result<(), String> {
    let key = front.key.get().ok_or("no key file")?;
    let mut secrets = key_lines(&key)?;
    secrets.push(hex(&UNKNOWN));
    for index in 0..2 {
        let printed = front.with(index, Node::printed)?;
        if let Some(secret) = secrets
            .iter()
            .find(|secret| printed.contains(secret.as_str()))
        {
            return Err(format!("node {index} printed the key {secret}"));
        }
    }
    Ok(())
}

#[then("the record names the key only by its key_id")]
fn named_by_key_id(front: &Front) -> Result<(), String> {
    let ids = front.key_ids.get().ok_or("keygen wrote no key")?;
    let mesh = ids.first().ok_or("keygen wrote no key")?;
    for index in 0..2 {
        front.prints(index, &format!("key {mesh}"))?;
    }
    front.prints(
        1,
        &format!("key {} is not one", smugglr_mesh::key_id(&UNKNOWN)),
    )
}

/// The 32 bytes a base64 key line holds.
fn decode(line: &str) -> Result<[u8; 32], String> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut bits = 0_u32;
    let mut count = 0;
    let mut bytes = Vec::new();
    for c in line.bytes().filter(|c| *c != b'=') {
        let value = ALPHABET
            .iter()
            .position(|a| *a == c)
            .ok_or("the key is not base64")?;
        bits = bits << 6 | u32::try_from(value).map_err(|err| err.to_string())?;
        count += 6;
        if count >= 8 {
            count -= 8;
            bytes.push(u8::try_from(bits >> count & 0xff).map_err(|err| err.to_string())?);
        }
    }
    bytes
        .try_into()
        .map_err(|_| "the key is not 32 bytes".to_owned())
}

/// The datagrams the sockets on UDP `port` have dropped for a full receive
/// buffer, as Linux counts them per socket in `/proc/net/udp`; `None` on a
/// system that keeps no count per socket. (macOS counts drops only for the
/// whole system, and there counts broadcasts it delivered to every socket on
/// the port: 200 sent and 200 received counted 109 drops.)
fn dropped_on(port: u16) -> Result<Option<u64>, String> {
    if !cfg!(target_os = "linux") {
        return Ok(None);
    }
    let table = std::fs::read_to_string("/proc/net/udp").map_err(|err| err.to_string())?;
    let port = format!(":{port:04X}");
    Ok(Some(
        table
            .lines()
            .skip(1)
            .filter(|line| {
                line.split_whitespace()
                    .nth(1)
                    .is_some_and(|local| local.ends_with(&port))
            })
            .filter_map(|line| line.split_whitespace().last()?.parse::<u64>().ok())
            .sum(),
    ))
}

/// Writes `rows` rows on node `index` in one statement.
fn write_rows(front: &Front, index: usize, rows: u64) -> Result<(), String> {
    front.sql(
        index,
        &format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {rows}) \
             INSERT INTO notes SELECT 'k' || i, 'row ' || i FROM n"
        ),
    )
}

/// Waits up to `within` for node `index` to hold `rows` rows.
fn holds_rows(front: &Front, index: usize, rows: i64, within: Duration) -> Result<(), String> {
    let path = front.with(index, |node| node.path.clone())?;
    let count = || {
        Connection::open(&path)
            .and_then(|db| {
                db.busy_timeout(Duration::from_secs(10))?;
                db.query_row("SELECT count(*) FROM notes", [], |row| row.get::<_, i64>(0))
            })
            .ok()
    };
    let started = Instant::now();
    while started.elapsed() < within {
        if count() == Some(rows) {
            return Ok(());
        }
        sleep(Duration::from_secs(1));
    }
    Err(format!("node {index} holds {:?} rows of {rows}", count()))
}

/// One flooding scenario at a time on the machine: they measure timing,
/// which another flood beside them would make a measure of the machine.
static FLOODING: Mutex<bool> = Mutex::new(false);

/// A flooding scenario's turn; ending the scenario ends its turn.
#[derive(Debug)]
struct Turn;

impl Turn {
    fn take() -> Arc<Self> {
        loop {
            {
                let mut busy = FLOODING.lock().unwrap_or_else(PoisonError::into_inner);
                if !*busy {
                    *busy = true;
                    return Arc::new(Self);
                }
            }
            sleep(Duration::from_millis(200));
        }
    }
}

impl Drop for Turn {
    fn drop(&mut self) {
        *FLOODING.lock().unwrap_or_else(PoisonError::into_inner) = false;
    }
}

#[given("two nodes running smugglr mesh")]
fn two_nodes(world: &World, front: &Front) -> Result<(), String> {
    front.turn.set(Turn::take());
    let key = key_file(world, front, "mesh.key")?;
    start_named(
        world,
        front,
        &[
            ("first", &key, FrontEnd::Mesh),
            ("second", &key, FrontEnd::Mesh),
        ],
        "",
    )
}

#[when("500,000 rows are written at once on the first node")]
fn a_flood(front: &Front) -> Result<(), String> {
    write_rows(front, 0, 500_000)
}

#[then("the second node holds all 500,000 once acknowledgements stop")]
fn the_second_holds_the_flood(front: &Front) -> Result<(), String> {
    holds_rows(front, 1, 500_000, Duration::from_secs(1_200))
}

#[then("neither node's socket reports a datagram dropped for a full buffer")]
fn nothing_dropped(front: &Front) -> Result<(), String> {
    // A change lost on the way, for a full buffer or any other cause,
    // reaches its node only as a resend, and the node says so.
    for index in 0..2 {
        let printed = front.with(index, Node::printed)?;
        if let Some(line) = printed
            .lines()
            .find(|line| line.contains("only as a resend"))
        {
            return Err(format!("node {index}: {line}"));
        }
    }
    let port = front.with(0, |node| node.port)?;
    match dropped_on(port)? {
        None | Some(0) => Ok(()),
        Some(dropped) => Err(format!(
            "the sockets on port {port} dropped {dropped} datagrams for a full buffer"
        )),
    }
}

#[when("50,000 rows are written at once on the first node")]
fn fifty_thousand(front: &Front) -> Result<(), String> {
    write_rows(front, 0, 50_000)
}

#[then(
    "the second node applies them in transactions of at most 20,000 changes, not one per change"
)]
fn applied_in_batches(front: &Front) -> Result<(), String> {
    holds_rows(front, 1, 50_000, Duration::from_secs(300))?;
    let printed = front.with(1, Node::printed)?;
    let batches: Vec<u64> = printed
        .lines()
        .filter_map(|line| line.strip_prefix("smugglr mesh: applied "))
        .filter_map(|rest| rest.split_whitespace().next()?.parse().ok())
        .collect();
    let (total, largest) = (batches.iter().sum::<u64>(), batches.iter().max().copied());
    if total != 50_000 {
        return Err(format!(
            "the second node applied {total} changes, not 50,000"
        ));
    }
    if largest > Some(20_000) {
        return Err(format!("a transaction applied {largest:?} changes"));
    }
    if batches.len() > 5_000 {
        return Err(format!("{} transactions for 50,000 changes", batches.len()));
    }
    Ok(())
}

/// What the relay saw of the two nodes' traffic.
#[derive(Debug, Default)]
struct Seen {
    /// The changes whose first copy the first node sent the relay.
    first_copies: HashSet<String>,
    /// Every change the first node sent the relay, first copy or resend.
    changes: HashSet<String>,
    /// When the relay last dropped a datagram carrying each change.
    lost: BTreeMap<String, Instant>,
    /// When a resend of each change first passed the relay after its loss.
    resent_at: BTreeMap<String, Instant>,
    /// When the second node first acknowledged each change.
    acked: BTreeMap<String, Instant>,
    /// The change to the last row a scenario writes.
    last_row: Option<String>,
    /// Datagrams the first node sent, and of them resends.
    sent: u64,
    resent: u64,
    /// The two counts as they stood at the last first copy.
    at_last_first: (u64, u64),
    /// Every resend either node sent: its row's key, its change and when.
    resends: Vec<(String, String, Instant)>,
    /// Every change either node sent, by its row's key.
    changes_by_key: BTreeMap<String, BTreeSet<String>>,
    /// When either node first acknowledged each change.
    acked_by_either: BTreeMap<String, Instant>,
}

/// Stands between two nodes on a network that carries no broadcast: each
/// node's peers setting names the relay, and the relay passes each node's
/// datagrams to the other, unicast, dropping one in ten while `lossy` is set.
#[derive(Debug)]
struct Relay {
    port: u16,
    stop: AtomicBool,
    lossy: AtomicBool,
    /// Lose the first copy of the change to the last row written.
    lose_last: AtomicBool,
    dropped: AtomicU64,
    seen: Mutex<Seen>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// The relay's socket on loopback, with the largest receive buffer the
/// system allows, so the relay drops only what it means to.
fn relay_socket() -> std::io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;
    let mut size = 8 << 20;
    while size >= 1 << 20 && socket.set_recv_buffer_size(size).is_err() {
        size /= 2;
    }
    socket.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into())?;
    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(Some(Duration::from_millis(50)))?;
    Ok(socket)
}

/// The message in `datagram`, sealed under `seal`.
fn open_with(datagram: &[u8], seal: &[u8; 32]) -> Option<Value> {
    let (nonce, sealed) = datagram.split_at_checked(24)?;
    let plain = XChaCha20Poly1305::new(seal.into())
        .decrypt(XNonce::from_slice(nonce), sealed)
        .ok()?;
    serde_json::from_slice(&plain).ok()
}

impl Relay {
    /// A relay for the nodes on `ports` (the first node's, then the
    /// second's), their datagrams sealed under the key derived from
    /// `shared_key`.
    fn start(ports: [u16; 2], shared_key: &[u8; 32], lossy: bool) -> Result<Arc<Self>, String> {
        let socket = relay_socket().map_err(|err| err.to_string())?;
        let relay = Arc::new(Self {
            port: socket.local_addr().map_err(|err| err.to_string())?.port(),
            stop: AtomicBool::new(false),
            lossy: AtomicBool::new(lossy),
            lose_last: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            seen: Mutex::new(Seen::default()),
        });
        let seal = blake3::derive_key("smugglr mesh 0.6 datagram seal", shared_key);
        // A reader drains the socket as datagrams arrive, so a system that
        // gives a small receive buffer does not drop what the relay means to
        // pass; the relay's own work happens beside it.
        let reader = socket.try_clone().map_err(|err| err.to_string())?;
        let (tx, rx) = std::sync::mpsc::channel::<(Vec<u8>, std::net::SocketAddr)>();
        let weak = Arc::downgrade(&relay);
        thread::spawn(move || {
            let mut buffer = vec![0; 65_536];
            while weak
                .upgrade()
                .is_some_and(|relay| !relay.stop.load(Ordering::Relaxed))
            {
                if let Ok((len, from)) = reader.recv_from(&mut buffer)
                    && tx.send((buffer[..len].to_vec(), from)).is_err()
                {
                    break;
                }
            }
        });
        let weak = Arc::downgrade(&relay);
        thread::spawn(move || {
            let mut random = 0x9e37_79b9_7f4a_7c15_u64;
            while let Some(relay) = weak.upgrade() {
                if relay.stop.load(Ordering::Relaxed) {
                    break;
                }
                let Ok((buffer, from)) = rx.recv_timeout(Duration::from_millis(50)) else {
                    continue;
                };
                let len = buffer.len();
                let (from_first, to) = match from.port() {
                    port if port == ports[0] => (true, ports[1]),
                    port if port == ports[1] => (false, ports[0]),
                    _ => continue,
                };
                random ^= random << 13;
                random ^= random >> 7;
                random ^= random << 17;
                let message = open_with(&buffer[..len], &seal);
                // The forced loss is looked at on every datagram, before the
                // random one, so it never goes unseen.
                let forced = relay.last_first_copy(message.as_ref(), from_first);
                let drop =
                    forced || (relay.lossy.load(Ordering::Relaxed) && random.is_multiple_of(10));
                relay.note(message, from_first, drop);
                if drop {
                    relay.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    let _ = socket.send_to(&buffer[..len], ("127.0.0.1", to));
                }
            }
        });
        Ok(relay)
    }

    /// Whether `message` is the first copy the relay sees of the change to
    /// the last row a scenario writes, which the relay then loses so its
    /// repair shows. It is the first the relay sees, not the maker's first
    /// send: the system may drop that before the relay reads it, and the
    /// change then first reaches the relay as a resend.
    fn last_first_copy(&self, message: Option<&Value>, from_first: bool) -> bool {
        let Some(message) = message.filter(|_| from_first) else {
            return false;
        };
        let envelope = message.get("envelope");
        let key = envelope.and_then(|envelope| envelope.get("key")?.get(0)?.as_str());
        let last = format!("'k{LAST_ROW}'");
        if !self.lose_last.load(Ordering::Relaxed)
            || message.get("t").and_then(Value::as_str) != Some("change")
            || key != Some(last.as_str())
        {
            return false;
        }
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if seen.last_row.is_some() {
            return false;
        }
        seen.last_row = envelope
            .and_then(|envelope| envelope.get("signature")?.as_str())
            .map(str::to_owned);
        true
    }

    fn note(&self, message: Option<Value>, from_first: bool, dropped: bool) {
        let Some(message) = message else {
            return;
        };
        let now = Instant::now();
        let text = |value: &Value| value.as_str().map(str::to_owned);
        let mut seen = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(envelope) = message.get("envelope")
            && let (Some(key), Some(change)) = (
                envelope
                    .get("key")
                    .and_then(|key| key.get(0))
                    .and_then(text),
                envelope.get("signature").and_then(text),
            )
        {
            seen.changes_by_key.entry(key).or_default().insert(change);
        }
        if message.get("resend").and_then(Value::as_bool) == Some(true)
            && let Some(envelope) = message.get("envelope")
            && let (Some(key), Some(change)) = (
                envelope
                    .get("key")
                    .and_then(|key| key.get(0))
                    .and_then(text),
                envelope.get("signature").and_then(text),
            )
        {
            seen.resends.push((key, change, now));
        }
        if message.get("t").and_then(Value::as_str) == Some("ack") {
            for change in message
                .get("changes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(text)
            {
                seen.acked_by_either.entry(change).or_insert(now);
            }
        }
        match message.get("t").and_then(Value::as_str) {
            Some("change") if from_first => {
                let Some(change) = message
                    .get("envelope")
                    .and_then(|envelope| text(envelope.get("signature")?))
                else {
                    return;
                };
                let resend = message.get("resend").and_then(Value::as_bool) == Some(true);
                seen.sent += 1;
                seen.changes.insert(change.clone());
                if resend {
                    seen.resent += 1;
                    if !dropped && seen.lost.contains_key(&change) {
                        seen.resent_at.entry(change.clone()).or_insert(now);
                    }
                } else {
                    seen.first_copies.insert(change.clone());
                    seen.at_last_first = (seen.sent, seen.resent);
                }
                if dropped {
                    // Measured from its latest loss: a resend lost too is a
                    // loss of its own.
                    seen.lost.insert(change, now);
                }
            }
            Some("ack") if !from_first => {
                for change in message
                    .get("changes")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(text)
                {
                    seen.acked.entry(change).or_insert(now);
                }
            }
            _ if from_first => seen.sent += 1,
            _ => {}
        }
    }
}

/// Two nodes, each naming the relay in its peers setting, the relay
/// dropping one datagram in ten when `lossy`; returns once each has heard
/// the other.
fn two_on_a_relay(world: &World, front: &Front, lossy: bool, first: &str) -> Result<(), String> {
    two_on_a_relay_as(world, front, lossy, first, FrontEnd::Mesh)
}

/// Two nodes as [`two_on_a_relay`] starts them, each started as `front_end`.
fn two_on_a_relay_as(
    world: &World,
    front: &Front,
    lossy: bool,
    first: &str,
    front_end: FrontEnd,
) -> Result<(), String> {
    front.turn.set(Turn::take());
    let key = key_file(world, front, "mesh.key")?;
    let line = key_lines(&key)?.pop().ok_or("the key file holds no key")?;
    let ports = [free_port()?, free_port()?];
    let relay = Relay::start(ports, &decode(&line)?, lossy)?;
    let peers = format!("peers = [\"127.0.0.1:{}\"]\n", relay.port);
    front.relay.set(relay);
    let more = format!("{peers}{first}");
    front.start_with(world, "first", &key, Some(ports[0]), front_end, &more)?;
    front.start_with(
        world,
        "second",
        &key,
        Some(ports[1]),
        FrontEnd::Mesh,
        &peers,
    )?;
    front.sql(0, "INSERT INTO notes VALUES ('w0', 'heard')")?;
    front.holds(1, "w0", "heard")?;
    front.sql(1, "INSERT INTO notes VALUES ('w1', 'heard')")?;
    front.holds(0, "w1", "heard")
}

#[given("two nodes running smugglr mesh that have heard each other")]
fn two_that_heard_each_other(world: &World, front: &Front) -> Result<(), String> {
    two_on_a_relay(world, front, false, "")
}

#[when(
    "50,000 rows are written at once on the first node, and the relay drops one datagram in ten"
)]
fn fifty_thousand_through_loss(front: &Front) -> Result<(), String> {
    front
        .relay
        .get()
        .ok_or("no relay")?
        .lossy
        .store(true, Ordering::Relaxed);
    write_rows(front, 0, 50_000)?;
    holds_rows(front, 1, 50_002, Duration::from_secs(300))
}

#[then("the first node sends each change's first copy unicast to the second node")]
fn first_copies_unicast(front: &Front) -> Result<(), String> {
    // The relay sits on no broadcast: what reaches it was sent to it,
    // unicast. A change whose first copy the system dropped before the relay
    // read it reaches the relay as a resend, to the same address.
    let relay = front.relay.get().ok_or("no relay")?;
    let seen = relay.seen.lock().unwrap_or_else(PoisonError::into_inner);
    let (firsts, changes) = (seen.first_copies.len(), seen.changes.len());
    if changes >= 50_000 && firsts * 100 >= changes * 99 {
        Ok(())
    } else {
        Err(format!(
            "the relay saw {changes} changes, {firsts} of them by their first copy"
        ))
    }
}

#[then("each lost copy is applied on the second node within one second of its loss")]
fn lost_copies_within_a_second(front: &Front) -> Result<(), String> {
    let relay = front.relay.get().ok_or("no relay")?;
    let seen = relay.seen.lock().unwrap_or_else(PoisonError::into_inner);
    if seen.lost.is_empty() {
        return Err("the relay lost no copy of a change".to_owned());
    }
    let late: Vec<Duration> = seen
        .lost
        .iter()
        .filter_map(|(change, lost)| match seen.acked.get(change) {
            Some(acked) => acked.checked_duration_since(*lost),
            None => Some(Duration::MAX),
        })
        .filter(|took| *took > Duration::from_secs(1))
        .collect();
    if late.is_empty() {
        Ok(())
    } else {
        let resent: Vec<Option<Duration>> = seen
            .lost
            .iter()
            .filter(|(change, lost)| match seen.acked.get(*change) {
                Some(acked) => acked
                    .checked_duration_since(**lost)
                    .is_some_and(|took| took > Duration::from_secs(1)),
                None => true,
            })
            .take(12)
            .map(|(change, lost)| {
                seen.resent_at
                    .get(change)
                    .and_then(|at| at.checked_duration_since(*lost))
            })
            .collect();
        Err(format!(
            "{} of {} lost copies took longer than a second, the slowest {:?}; resent after {resent:?}; resends {} of {}",
            late.len(),
            seen.lost.len(),
            late.iter().max(),
            seen.resent,
            seen.sent
        ))
    }
}

#[then(
    "while first copies are still waiting to be sent, resends take no more than a quarter of what the first node sends"
)]
fn resends_a_quarter_at_most(front: &Front) -> Result<(), String> {
    let relay = front.relay.get().ok_or("no relay")?;
    let seen = relay.seen.lock().unwrap_or_else(PoisonError::into_inner);
    // Counted up to the last first copy: until then first copies were
    // waiting to be sent.
    let (sent, resent) = seen.at_last_first;
    if resent * 4 <= sent {
        Ok(())
    } else {
        Err(format!(
            "{resent} of {sent} datagrams were resends while first copies were waiting"
        ))
    }
}

#[given("two nodes running smugglr mesh, and a relay that drops one datagram in ten")]
fn two_through_a_lossy_relay(world: &World, front: &Front) -> Result<(), String> {
    two_on_a_relay(world, front, false, "")?;
    front
        .relay
        .get()
        .ok_or("no relay")?
        .lossy
        .store(true, Ordering::Relaxed);
    Ok(())
}

#[then("the first node's send window never halves for a lost datagram")]
fn the_window_holds(front: &Front) -> Result<(), String> {
    holds_rows(front, 1, 50_002, Duration::from_secs(300))?;
    let relay = front.relay.get().ok_or("no relay")?;
    if relay.dropped.load(Ordering::Relaxed) == 0 {
        return Err("the relay dropped no datagram".to_owned());
    }
    let printed = front.with(0, Node::printed)?;
    let congestion = ["the send buffer is full", "acknowledgements slowing"];
    match printed.lines().find(|line| {
        line.contains("send window halved") && !congestion.iter().any(|why| line.ends_with(why))
    }) {
        Some(line) => Err(format!("the first node: {line}")),
        None => Ok(()),
    }
}

#[given(
    "two nodes running smugglr mesh on a relay that passes no broadcast, each naming the other in its peers setting"
)]
fn two_named_in_peers(world: &World, front: &Front) -> Result<(), String> {
    two_on_a_relay(world, front, false, "")
}

#[when("a row is written on the first node")]
fn a_row_on_the_first_node(front: &Front) -> Result<(), String> {
    front.sql(0, "INSERT INTO notes VALUES ('n9', 'across')")
}

/// The last row a 50,000-row scenario writes.
const LAST_ROW: u64 = 50_000;

#[when("50,000 rows are written at once on the first node and every first copy has been sent")]
fn fifty_thousand_all_sent(front: &Front) -> Result<(), String> {
    write_rows(front, 0, LAST_ROW)?;
    let relay = front.relay.get().ok_or("no relay")?;
    // A change the relay has seen at all, first copy or resend, has had its
    // first send.
    let sent = || {
        relay
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .changes
            .len()
    };
    wait(Duration::from_secs(300), || sent() >= 50_000)
        .map_err(|()| format!("the first node sent first copies of {} changes", sent()))
}

#[then("the first node keeps resending until the second node holds all 50,000 rows")]
fn repair_goes_on(front: &Front) -> Result<(), String> {
    let relay = front.relay.get().ok_or("no relay")?;
    let resent = || {
        relay
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .resent
    };
    let before = resent();
    let path = front.with(1, |node| node.path.clone())?;
    let missing = Connection::open(&path)
        .and_then(|db| {
            db.query_row("SELECT 50002 - count(*) FROM notes", [], |row| {
                row.get::<_, i64>(0)
            })
        })
        .map_err(|err| err.to_string())?;
    holds_rows(front, 1, 50_002, Duration::from_secs(120))?;
    if missing > 0 && resent() == before {
        return Err(format!(
            "the second node lacked {missing} rows when first sends ran out, and nothing was resent"
        ));
    }
    Ok(())
}

#[given(
    "two nodes running smugglr mesh with the backpressure setting at 1 MB, and a relay that drops one datagram in ten"
)]
fn two_with_a_small_line(world: &World, front: &Front) -> Result<(), String> {
    two_on_a_relay(world, front, false, "backpressure = \"1 MB\"\n")?;
    let relay = front.relay.get().ok_or("no relay")?;
    relay.lose_last.store(true, Ordering::Relaxed);
    relay.lossy.store(true, Ordering::Relaxed);
    Ok(())
}

#[then("the second node holds the last row written within one second of its latest loss")]
fn the_last_row_within_a_second(front: &Front) -> Result<(), String> {
    holds_rows(front, 1, 50_002, Duration::from_secs(300))?;
    let relay = front.relay.get().ok_or("no relay")?;
    let seen = relay.seen.lock().unwrap_or_else(PoisonError::into_inner);
    let last = seen
        .last_row
        .clone()
        .ok_or("the relay saw no copy of the last row")?;
    let lost = seen
        .lost
        .get(&last)
        .ok_or("the relay never lost the last row")?;
    let acked = seen
        .acked
        .get(&last)
        .ok_or("the second node never acknowledged the last row")?;
    match acked.checked_duration_since(*lost) {
        Some(took) if took > Duration::from_secs(1) => Err(format!(
            "the last row was repaired {took:?} after its latest loss"
        )),
        _ => Ok(()),
    }
}

#[given("a node whose last run ended longer ago than the sunset")]
fn a_node_back_from_away(world: &World, front: &Front) -> Result<(), String> {
    let key = key_file(world, front, "mesh.key")?;
    // A sunset of 2 seconds, so a short absence is longer than it.
    start_named(
        world,
        front,
        &[
            ("away", &key, FrontEnd::Mesh),
            ("stayed", &key, FrontEnd::Mesh),
        ],
        "graveyard = 2\n",
    )?;
    // The node notes that it is running as soon as it announces itself.
    sleep(Duration::from_secs(1));
    front.stop(0)?;
    sleep(Duration::from_secs(4));
    Ok(())
}

#[when("it starts smugglr mesh")]
fn it_starts_again(front: &Front) -> Result<(), String> {
    front.restart(0)
}

#[then("it prints that it has been away longer than the sunset")]
fn it_says_it_was_away(front: &Front) -> Result<(), String> {
    front.prints(0, "longer than the sunset")
}

#[then("it names smugglr stash and smugglr retrieve as the way to catch up")]
fn it_names_the_catch_up(front: &Front) -> Result<(), String> {
    front.prints(0, "smugglr stash")?;
    front.prints(0, "smugglr retrieve")
}

#[then("it keeps running and applies new changes")]
fn it_keeps_running(front: &Front) -> Result<(), String> {
    sleep(Duration::from_secs(1));
    front.sql(1, "INSERT INTO notes VALUES ('n7', 'after')")?;
    front.holds(0, "n7", "after")
}

#[given("two nodes running smugglr mesh with --sync-mode stop")]
fn two_nodes_that_stop(world: &World, front: &Front) -> Result<(), String> {
    two_on_a_relay_as(world, front, false, "", FrontEnd::MeshStop)?;
    // The row both will change, the same on each before either does.
    front.sql(0, "INSERT INTO notes VALUES ('c1', 'base')")?;
    front.holds(1, "c1", "base")
}

#[when("both nodes change the same row at once")]
fn both_change_it(front: &Front) -> Result<(), String> {
    front.sql(0, "UPDATE notes SET body = 'left' WHERE id = 'c1'")?;
    front.sql(1, "UPDATE notes SET body = 'right' WHERE id = 'c1'")?;
    for index in 0..2 {
        front.prints(index, "stopped notes")?;
    }
    // Past several round trips, when a change not acknowledged would have
    // been resent.
    sleep(Duration::from_secs(3));
    Ok(())
}

#[then("each node names the conflict in its record once")]
fn named_once(front: &Front) -> Result<(), String> {
    for index in 0..2 {
        let printed = front.with(index, Node::printed)?;
        let named = printed.matches("stopped notes").count();
        if named != 1 {
            return Err(format!("node {index} named the conflict {named} times"));
        }
    }
    Ok(())
}

#[then("neither node resends that change after it is acknowledged")]
fn not_resent(front: &Front) -> Result<(), String> {
    let relay = front.relay.get().ok_or("no relay")?;
    let seen = relay.seen.lock().unwrap_or_else(PoisonError::into_inner);
    // Each change to the row, the stopped ones too, was acknowledged.
    let changes = seen.changes_by_key.get("'c1'").cloned().unwrap_or_default();
    if changes.len() < 3 {
        return Err(format!(
            "the relay saw {} changes to the row",
            changes.len()
        ));
    }
    if let Some(change) = changes
        .iter()
        .find(|change| !seen.acked_by_either.contains_key(*change))
    {
        return Err(format!("change {change} to the row was never acknowledged"));
    }
    // A resend already on its way when the acknowledgement passed the relay
    // is not one sent after it: a quarter second is allowed for the
    // acknowledgement to reach its maker.
    let late = seen.resends.iter().find(|(key, change, at)| {
        key == "'c1'"
            && seen
                .acked_by_either
                .get(change)
                .is_some_and(|acked| *acked + Duration::from_millis(250) < *at)
    });
    match late {
        Some((_, change, _)) => Err(format!(
            "change {change} was resent after it was acknowledged"
        )),
        None => Ok(()),
    }
}

/// The graveyard, in seconds, of the replay scenario's nodes: short, so a
/// logged change is past its own sunset by the time it is replayed.
const REPLAY_GRAVEYARD: u64 = 6;

impl Front {
    /// Runs smugglr with `args` as node `index` runs, on its database, and
    /// waits for it to exit.
    fn run_as(&self, index: usize, args: &[&str]) -> Result<std::process::Output, String> {
        let path = self.with(index, |node| node.path.clone())?;
        let folder = path.parent().ok_or("a node's database has no folder")?;
        let mut command = Command::new(env!("CARGO_BIN_EXE_smugglr"));
        command
            .arg("mesh")
            .arg(&path)
            .args(args)
            .env("XDG_CONFIG_HOME", folder.join("config"))
            .env("PATH", search_path(&folder.join("bin")))
            .env("COMPUTERNAME", "replayer");
        command
            .output()
            .map_err(|err| format!("failed to start smugglr: {err}"))
    }

    /// Waits for node `index` to hold no note `id`.
    fn lacks(&self, index: usize, id: &str) -> Result<(), String> {
        let path = self.with(index, |node| node.path.clone())?;
        wait(SOON, || {
            Connection::open(&path)
                .and_then(|db| {
                    db.busy_timeout(Duration::from_secs(5))?;
                    db.query_row("SELECT count(*) FROM notes WHERE id = ?1", [id], |row| {
                        row.get::<_, i64>(0)
                    })
                })
                .is_ok_and(|count| count == 0)
        })
        .map_err(|()| format!("node {index} still holds {id}"))
    }
}

#[given("a mesh node whose dropped log holds a change")]
fn a_node_with_a_dropped_change(world: &World, front: &Front) -> Result<(), String> {
    let key = key_file(world, front, "replay.key")?;
    start_named(
        world,
        front,
        &[("ra", &key, FrontEnd::Mesh), ("rb", &key, FrontEnd::Mesh)],
        &format!("graveyard = {REPLAY_GRAVEYARD}\n"),
    )?;
    front.key.set(key);
    front.sql(0, "INSERT INTO notes VALUES ('n1', 'first')")?;
    front.holds(1, "n1", "first")?;
    // Deleted, then made again inside the graveyard window: the second
    // node drops the insert and keeps it in its dropped log.
    front.sql(0, "DELETE FROM notes WHERE id = 'n1'")?;
    front.lacks(1, "n1")?;
    front.sql(0, "INSERT INTO notes VALUES ('n1', 'again')")?;
    front.prints(1, "dropped a change to notes")?;
    front.stop(0)?;
    front.stop(1)?;
    // Past the logged change's own sunset: only a fresh one lets it in.
    sleep(Duration::from_secs(REPLAY_GRAVEYARD + 2));
    Ok(())
}

/// The change hash the record of node `index` names its first drop by.
fn dropped_hash(front: &Front, index: usize) -> Result<String, String> {
    let printed = front.with(index, Node::printed)?;
    printed
        .lines()
        .find(|line| line.contains("dropped a change to notes"))
        .and_then(|line| line.rsplit_once("(change ")?.1.strip_suffix(')'))
        .map(str::to_owned)
        .ok_or_else(|| format!("no drop named by its change hash in {printed:?}"))
}

#[when("the agent runs smugglr mesh with --replay and that change's hash")]
fn replays_it(front: &Front) -> Result<(), String> {
    let key = front.key.get().ok_or("no key file")?;
    let hash = dropped_hash(front, 1)?;
    let key = key.display().to_string();
    let output = front.run_as(1, &["--key-file", &key, "--replay", &hash])?;
    front.refused.set(output);
    Ok(())
}

#[then("the change is sent again with a fresh sunset")]
fn sent_again(front: &Front) -> Result<(), String> {
    let output = front.refused.get().ok_or("smugglr was not run")?;
    let printed = String::from_utf8_lossy(&output.stdout);
    if !output.status.success() || !printed.contains("replayed change") {
        return Err(format!(
            "the replay exited {}, printing {printed:?} and saying {:?}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    // Past its own sunset, the change is applied only with a fresh one.
    front.holds_within(1, "n1", "again", Duration::ZERO)
}

#[then("running smugglr mesh with --replay and a change hash not in the dropped log exits 2")]
fn unknown_hash_exits_2(front: &Front) -> Result<(), String> {
    let key = front.key.get().ok_or("no key file")?;
    let key = key.display().to_string();
    let unknown = hex(&[0_u8; 32]);
    let output = front.run_as(1, &["--key-file", &key, "--replay", &unknown])?;
    let said = String::from_utf8_lossy(&output.stderr);
    if output.status.code() == Some(2) && said.contains("dropped log holds no change") {
        Ok(())
    } else {
        Err(format!(
            "an unknown hash exited {:?}, saying {said:?}",
            output.status.code()
        ))
    }
}
