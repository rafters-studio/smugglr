//! Steps for the mesh: nodes run in threads on SQLite files in the
//! scenario's folder, each broadcasting on its own port on this machine. A
//! relay listens beside each node on its port, passes each datagram a node
//! sends to every other node's port by broadcast, captures every datagram,
//! and drops those a scenario says to.

use std::collections::{BTreeMap, HashSet};
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle, sleep};
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
use smugglr_core::plan::SyncMode;
use smugglr_core::row_hash::row_hash;
use smugglr_core::shape::{Column, Stamp};
use smugglr_core::steering::Steering;
use smugglr_mesh::{BACKPRESSURE, K, Settings, key_id};
use socket2::{Domain, Protocol, Socket, Type};

/// The key every node of a scenario holds, unless it says otherwise.
const KEY: [u8; 32] = [7; 32];

/// The key a scenario's nodes held before [`KEY`].
const PREVIOUS: [u8; 32] = [5; 32];

/// A key no node of a scenario holds.
const STRANGER: [u8; 32] = [9; 32];

/// The notes table every node's database holds.
const NOTES: &str = "CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)";

/// The graveyard window most scenarios run with: no sunset passes.
const GRAVEYARD: u64 = 600;

/// How long a scenario waits for a change that is sent at once, echoes
/// and acknowledgements included.
const SOON: Duration = Duration::from_secs(10);

static NEXT: AtomicUsize = AtomicUsize::new(0);

/// One datagram the relay saw: from which node's port, to which, whether
/// it was dropped, and the message it holds.
#[derive(Debug, Clone)]
struct Seen {
    /// The datagram's number, the same for each node it went to.
    n: usize,
    from: u16,
    to: u16,
    at: Instant,
    dropped: bool,
    bytes: Vec<u8>,
    message: Option<Value>,
}

/// Says whether to drop a datagram, given what the relay saw before it.
type Filter = Box<dyn FnMut(&Seen, &[Seen]) -> bool + Send>;

/// Listens beside every node on its port and passes what each node sends to
/// every other node's port, and keeps it. It passes a datagram on from the
/// socket beside the node it goes to, so the datagram reaches that port as
/// a node's own broadcast does, and it knows its own datagrams by their
/// nonces.
struct Relay {
    /// The socket beside each node, by its port.
    sockets: Mutex<BTreeMap<u16, Arc<UdpSocket>>>,
    /// The nonces of the datagrams the relay sent itself.
    sent: Mutex<HashSet<Vec<u8>>>,
    seen: Mutex<Vec<Seen>>,
    filter: Mutex<Option<Filter>>,
    count: AtomicUsize,
    stop: AtomicBool,
}

impl Relay {
    fn start() -> Arc<Self> {
        Arc::new(Self {
            sockets: Mutex::new(BTreeMap::new()),
            sent: Mutex::new(HashSet::new()),
            seen: Mutex::new(Vec::new()),
            filter: Mutex::new(None),
            count: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
        })
    }

    /// Listens on a port of its own choosing, which the node that will use
    /// it shares; holding the port from the start, no other scenario can
    /// take it.
    fn listen(self: &Arc<Self>) -> Result<u16, String> {
        let socket = Arc::new(shared(0).map_err(|err| err.to_string())?);
        let port = socket.local_addr().map_err(|err| err.to_string())?.port();
        self.sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(port, socket.clone());
        let relay = self.clone();
        thread::spawn(move || relay.pass(&socket, port));
        Ok(port)
    }

    /// Sends `datagram` to the node on `port`, from the socket beside it.
    fn send(&self, datagram: &[u8], port: u16) -> Result<(), String> {
        let socket = self
            .sockets
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&port)
            .cloned()
            .ok_or_else(|| format!("no node on port {port}"))?;
        self.sent
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(datagram[..datagram.len().min(24)].to_vec());
        socket
            .send_to(datagram, ("255.255.255.255", port))
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    fn pass(&self, socket: &UdpSocket, port: u16) {
        let mut buffer = vec![0; 65_536];
        while !self.stop.load(Ordering::Relaxed) {
            let Ok((len, _)) = socket.recv_from(&mut buffer) else {
                continue;
            };
            let bytes = buffer[..len].to_vec();
            // What the relay sent itself is not a node's.
            if self
                .sent
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&bytes[..len.min(24)])
            {
                continue;
            }
            let message = open(&bytes);
            let ports: Vec<u16> = self
                .sockets
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .keys()
                .copied()
                .collect();
            let mut to: Vec<u16> = ports.into_iter().filter(|to| *to != port).collect();
            // A datagram with no node to go to is still seen, going nowhere.
            if to.is_empty() {
                to.push(0);
            }
            let n = self.count.fetch_add(1, Ordering::Relaxed) + 1;
            for to in to {
                let mut seen = Seen {
                    n,
                    from: port,
                    to,
                    at: Instant::now(),
                    dropped: false,
                    bytes: bytes.clone(),
                    message: message.clone(),
                };
                let dropped = {
                    let log = self.seen.lock().unwrap_or_else(PoisonError::into_inner);
                    let mut filter = self.filter.lock().unwrap_or_else(PoisonError::into_inner);
                    filter.as_mut().is_some_and(|drop| drop(&seen, &log))
                };
                seen.dropped = dropped;
                if !dropped && to != 0 {
                    let _ = self.send(&bytes, to);
                }
                self.seen
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(seen);
            }
        }
    }

    /// Sends `message`, sealed under the shared key `key`, to the node on
    /// `port`.
    fn inject(&self, message: &Value, key: &[u8; 32], port: u16) -> Result<(), String> {
        let cipher = XChaCha20Poly1305::new((&sealing(key)).into());
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let sealed = cipher
            .encrypt(&nonce, message.to_string().as_bytes())
            .map_err(|err| err.to_string())?;
        let mut datagram = nonce.to_vec();
        datagram.extend(sealed);
        self.send(&datagram, port)
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// A UDP socket on `port` that the node using it shares.
fn shared(port: u16) -> std::io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_broadcast(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(Some(Duration::from_millis(50)))?;
    Ok(socket)
}

/// The key a node seals datagrams with, derived from the shared key `key`
/// as the wire says.
fn sealing(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("smugglr mesh 0.6 datagram seal", key)
}

/// The key a node signs envelopes with, derived from the shared key `key`
/// as the wire says.
fn signing(key: &[u8; 32]) -> [u8; 32] {
    blake3::derive_key("smugglr mesh 0.6 envelope signature", key)
}

/// The message a datagram holds, opened under any key a scenario uses.
fn open(datagram: &[u8]) -> Option<Value> {
    let (nonce, sealed) = datagram.split_at_checked(24)?;
    [KEY, PREVIOUS, STRANGER].iter().find_map(|key| {
        let plain = XChaCha20Poly1305::new((&sealing(key)).into())
            .decrypt(XNonce::from_slice(nonce), sealed)
            .ok()?;
        serde_json::from_slice(&plain).ok()
    })
}

/// One running node.
struct Node {
    path: PathBuf,
    config: PathBuf,
    port: u16,
    settings: Settings,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<(), String>>>,
}

impl Node {
    fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }

    /// Runs the node in a thread of its own until it is stopped.
    fn run(&mut self) {
        let stop = Arc::new(AtomicBool::new(false));
        let (address, settings, stopping) = (
            self.path.display().to_string(),
            self.settings.clone(),
            stop.clone(),
        );
        self.thread = Some(thread::spawn(move || {
            let mut place = smugglr_place::open(&address).map_err(|err| err.to_string())?;
            smugglr_mesh::run(place.as_mut(), &settings, &stopping).map_err(|err| {
                eprintln!("{}: {err}", settings.node);
                err.to_string()
            })
        }));
        self.stop = stop;
    }

    /// The bytes of the files in the node's outbox.
    fn outbox(&self) -> u64 {
        let Ok(folders) = std::fs::read_dir(self.config.join("mesh")) else {
            return 0;
        };
        folders
            .flatten()
            .filter_map(|folder| std::fs::read_dir(folder.path().join("outbox")).ok())
            .flat_map(Iterator::flatten)
            .filter_map(|file| file.metadata().ok())
            .map(|metadata| metadata.len())
            .sum()
    }
}

/// A scenario's nodes and their relay.
struct Net {
    /// The scenario's own name, unique in this run.
    name: String,
    folder: PathBuf,
    relay: Arc<Relay>,
    nodes: Mutex<Vec<Node>>,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net")
            .field("folder", &self.folder)
            .finish_non_exhaustive()
    }
}

impl Drop for Net {
    fn drop(&mut self) {
        for node in self.nodes.get_mut().unwrap_or_else(PoisonError::into_inner) {
            node.stop();
        }
        self.relay.stop.store(true, Ordering::Relaxed);
    }
}

impl Net {
    fn new() -> Result<Self, String> {
        let name = format!(
            "mesh-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(&name);
        let _ = std::fs::remove_dir_all(&folder);
        std::fs::create_dir_all(&folder).map_err(|err| err.to_string())?;
        Ok(Self {
            name,
            folder,
            relay: Relay::start(),
            nodes: Mutex::new(Vec::new()),
        })
    }

    fn nodes(&self) -> std::sync::MutexGuard<'_, Vec<Node>> {
        self.nodes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Starts a node holding `keys`, its database made by `sql`, and returns
    /// its index.
    fn start(&self, keys: &[[u8; 32]], graveyard: u64, sql: &str) -> Result<usize, String> {
        self.start_with(keys, graveyard, BACKPRESSURE, sql)
    }

    /// Starts a node as [`Net::start`] does, its outbox held to
    /// `backpressure` bytes.
    fn start_with(
        &self,
        keys: &[[u8; 32]],
        graveyard: u64,
        backpressure: u64,
        sql: &str,
    ) -> Result<usize, String> {
        let index = self.nodes().len();
        let path = self.folder.join(format!("node{index}.db"));
        Connection::open(&path)
            .and_then(|db| db.execute_batch(&format!("{NOTES}; {sql}")))
            .map_err(|err| err.to_string())?;
        let config = self.folder.join(format!("config{index}"));
        let port = self.relay.listen()?;
        let settings = Settings {
            store: Arc::new(smugglr_memory::Files::new(&config)),
            keys: keys.to_vec(),
            // Scenarios run side by side on this machine: each node's name
            // is its own.
            node: format!("{}-node{index}", self.name),
            port,
            graveyard,
            sync_mode: SyncMode::Stop,
            steering: Steering::default(),
            ignore: Vec::new(),
            k: K,
            backpressure,
        };
        let mut node = Node {
            path,
            config: config.clone(),
            port,
            settings,
            stop: Arc::new(AtomicBool::new(false)),
            thread: None,
        };
        node.run();
        self.nodes().push(node);
        // A node's first look notes every row as it is, and sends nothing;
        // a row written before it would never be sent. Its look is done
        // once it keeps what it saw.
        wait(SOON, || {
            std::fs::read_dir(&config).is_ok_and(|mut entries| entries.next().is_some())
        })
        .map_err(|()| format!("node {index} did not look at its rows"))?;
        Ok(index)
    }

    /// Starts `count` nodes holding [`KEY`] and waits until each has heard
    /// every other: there is no member list, so each writes one row and
    /// every node holds every one, each having heard the others' changes
    /// and acknowledgements.
    fn start_many(&self, count: usize, sql: &str) -> Result<(), String> {
        for _ in 0..count {
            self.start(&[KEY], GRAVEYARD, sql)?;
        }
        for index in 0..count {
            self.sql(
                index,
                &format!("INSERT INTO notes VALUES ('w{index}', 'heard')"),
            )?;
        }
        for index in 0..count {
            for row in 0..count {
                self.holds(index, &format!("w{row}"), "heard", SOON)?;
            }
        }
        Ok(())
    }

    fn path(&self, index: usize) -> Result<PathBuf, String> {
        self.nodes()
            .get(index)
            .map(|node| node.path.clone())
            .ok_or_else(|| format!("no node {index}"))
    }

    fn port(&self, index: usize) -> Result<u16, String> {
        self.nodes()
            .get(index)
            .map(|node| node.port)
            .ok_or_else(|| format!("no node {index}"))
    }

    fn sql(&self, index: usize, sql: &str) -> Result<(), String> {
        Connection::open(self.path(index)?)
            .and_then(|db| {
                db.busy_timeout(Duration::from_secs(5))?;
                db.execute_batch(sql)
            })
            .map_err(|err| err.to_string())
    }

    /// The body of note `id` on node `index`; `None` when it holds none.
    fn body(&self, index: usize, id: &str) -> Result<Option<String>, String> {
        let db = Connection::open(self.path(index)?).map_err(|err| err.to_string())?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|err| err.to_string())?;
        let mut statement = db
            .prepare("SELECT body FROM notes WHERE id = ?1")
            .map_err(|err| err.to_string())?;
        let mut rows = statement
            .query_map([id], |row| row.get::<_, String>(0))
            .map_err(|err| err.to_string())?;
        rows.next().transpose().map_err(|err| err.to_string())
    }

    /// Waits up to `within` for node `index` to hold note `id` with `body`.
    fn holds(&self, index: usize, id: &str, body: &str, within: Duration) -> Result<(), String> {
        wait(within, || {
            self.body(index, id).ok().flatten().as_deref() == Some(body)
        })
        .map_err(|()| {
            format!(
                "node {index} holds {:?} for note {id}, not {body:?}",
                self.body(index, id)
            )
        })
    }

    /// A change of note `id` from `before` (`None`: an insert) to `after`,
    /// made by `maker`, signed with `key`, as the wire carries it from
    /// `maker`.
    fn change(
        &self,
        id: &str,
        before: Option<&str>,
        after: &str,
        key: &[u8; 32],
    ) -> Result<Value, String> {
        let quoted = |body: &str| vec![format!("'{id}'"), format!("'{body}'")];
        let hash = |values: &[String]| {
            let values: Vec<&str> = values.iter().map(String::as_str).collect();
            row_hash(&values)
        };
        let from = before.map(|before| hash(&quoted(before)));
        let values = quoted(after);
        let mut place = smugglr_place::open(&self.path(0)?.display().to_string())
            .map_err(|err| err.to_string())?;
        let table = place
            .tables()
            .map_err(|err| err.to_string())?
            .into_iter()
            .find(|table| table.name == "notes")
            .ok_or("no notes table")?;
        let columns: Vec<Column> = table
            .columns
            .iter()
            .zip(&table.types)
            .map(|(name, kind)| Column {
                name: name.clone(),
                kind: kind.clone(),
            })
            .collect();
        let mut envelope = Envelope {
            v: VERSION,
            node: "maker".to_owned(),
            change: Change {
                op: if from.is_some() { Op::Upsert } else { Op::Up },
                table: "notes".to_owned(),
                key: vec![format!("'{id}'")],
                from,
                to: Some(hash(&values)),
                values,
            },
            targets: Vec::new(),
            sunset: now() + GRAVEYARD,
            txn: None,
            key_id: key_id(key),
            stamp: Stamp::of(&columns),
            signature: [0; 32],
        };
        envelope.sign(&signing(key));
        Ok(json!({"t": "change", "sender": "maker", "envelope": envelope.to_json()}))
    }

    /// The changes node `index` sent, as the relay saw them, once each.
    fn sent_by(&self, index: usize) -> Result<Vec<(Instant, Envelope)>, String> {
        let port = self.port(index)?;
        let mut sent = Vec::new();
        let mut last = 0;
        for seen in self.relay.seen() {
            if seen.from != port || seen.n == last {
                continue;
            }
            if let Some(envelope) = changed(&seen) {
                last = seen.n;
                sent.push((seen.at, envelope));
            }
        }
        Ok(sent)
    }
}

/// The envelope a change datagram carries.
fn changed(seen: &Seen) -> Option<Envelope> {
    let message = seen.message.as_ref()?;
    (message.get("t")?.as_str()? == "change")
        .then(|| Envelope::from_json(message.get("envelope")?))
        .flatten()
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
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

/// A scenario's mesh.
#[derive(Debug, Default, ScenarioState)]
pub(crate) struct Mesh {
    net: Slot<Arc<Net>>,
    /// When the change a scenario watches was made.
    made: Slot<Instant>,
}

#[fixture]
pub(crate) fn mesh() -> Mesh {
    Mesh::default()
}

impl Mesh {
    fn net(&self) -> Result<Arc<Net>, String> {
        if let Some(net) = self.net.get() {
            return Ok(net);
        }
        let net = Arc::new(Net::new()?);
        self.net.set(net.clone());
        Ok(net)
    }
}

#[given("three nodes running smugglr mesh with the same key file")]
fn three_nodes(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.start_many(3, "")
}

#[given("two nodes running smugglr mesh with the same key file")]
fn two_nodes_holding_a_note(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?
        .start_many(2, "INSERT INTO notes VALUES ('n1', 'before')")
}

#[when("a row is written on the first node's database")]
fn a_row_on_the_first(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?
        .sql(0, "INSERT INTO notes VALUES ('n9', 'written')")
}

#[then("the other two nodes hold that row without anyone running a sync")]
fn the_other_two_hold_it(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.holds(1, "n9", "written", SOON)?;
    net.holds(2, "n9", "written", SOON)
}

#[when("a program other than smugglr edits a row in the first node's database")]
fn another_program_edits(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?
        .sql(0, "UPDATE notes SET body = 'edited' WHERE id = 'n1'")
}

#[then("the second node holds the edited row")]
fn the_second_holds_the_edit(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.holds(1, "n1", "edited", SOON)
}

#[given("three nodes running smugglr mesh, and the first send of a change to the third node lost")]
fn the_first_send_to_the_third_lost(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.start_many(3, "")?;
    let (maker, third) = (net.port(0)?, net.port(2)?);
    // The maker's first copy to the third node is lost, and so is every
    // echo to it, so only a resend can bring it.
    let watched =
        |seen: &Seen| changed(seen).is_some_and(|envelope| envelope.change.key == ["'n9'"]);
    let filter: Filter = Box::new(move |seen, before| {
        if seen.to != third || !watched(seen) {
            return false;
        }
        let first = !before
            .iter()
            .any(|other| other.to == third && other.from == maker && watched(other));
        seen.from != maker || first
    });
    *net.relay
        .filter
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(filter);
    net.sql(0, "INSERT INTO notes VALUES ('n9', 'resent')")
}

#[when("the sender's resend reaches the third node")]
fn the_resend_reaches_the_third(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let (maker, third) = (net.port(0)?, net.port(2)?);
    wait(Duration::from_secs(20), || {
        net.relay.seen().iter().any(|seen| {
            seen.from == maker
                && seen.to == third
                && !seen.dropped
                && changed(seen).is_some_and(|envelope| envelope.change.key == ["'n9'"])
        })
    })
    .map_err(|()| "no resend reached the third node".to_owned())
}

#[then("the third node holds the change")]
fn the_third_holds_it(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.holds(2, "n9", "resent", SOON)
}

#[given("a node that has not acknowledged a change")]
fn a_peer_that_never_acknowledges(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.start(&[KEY], 12, "")?;
    sleep(Duration::from_millis(500));
    // A peer the sender hears once, acknowledging some other node's
    // change, and that never acknowledges the sender's.
    // Said a few times over a second, so one lost datagram does not leave
    // the sender with no peer to resend to.
    for _ in 0..4 {
        net.relay.inject(
            &json!({"t": "ack", "sender": "silent", "maker": "elsewhere", "changes": [hex(&[0; 32])]}),
            &KEY,
            net.port(0)?,
        )?;
        sleep(Duration::from_millis(250));
    }
    net.sql(0, "INSERT INTO notes VALUES ('n9', 'unacknowledged')")?;
    wait(SOON, || !net.sent_by(0).unwrap_or_default().is_empty())
        .map_err(|()| "the sender sent no change".to_owned())
}

#[when("the change's sunset passes")]
fn the_sunset_passes(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let sunset = net
        .sent_by(0)?
        .first()
        .map(|(_, envelope)| envelope.sunset)
        .ok_or("no change was sent")?;
    while now() <= sunset {
        sleep(Duration::from_millis(200));
    }
    mesh.made.set(Instant::now());
    Ok(())
}

#[then("the sender no longer sends that change")]
fn the_sender_stops(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let passed = mesh.made.get().ok_or("the sunset did not pass")?;
    let sent = net.sent_by(0)?;
    let signature = sent.first().map(|(_, envelope)| envelope.signature);
    let before = sent
        .iter()
        .filter(|(at, envelope)| *at < passed && Some(envelope.signature) == signature)
        .count();
    if before < 2 {
        return Err("the sender never resent the change before its sunset".to_owned());
    }
    // The next resend after the one before the sunset would be due 20 s
    // after it.
    sleep(Duration::from_secs(20));
    let after = net
        .sent_by(0)?
        .into_iter()
        .filter(|(at, envelope)| *at >= passed && Some(envelope.signature) == signature)
        .count();
    if after == 0 {
        Ok(())
    } else {
        Err(format!(
            "the sender sent the change {after} times after its sunset"
        ))
    }
}

#[given("a node running smugglr mesh with one key file")]
fn a_node_with_one_key(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.start(
        &[KEY],
        GRAVEYARD,
        "INSERT INTO notes VALUES ('n1', 'before')",
    )?;
    sleep(Duration::from_secs(1));
    Ok(())
}

#[when("it receives a change signed with a different key")]
fn a_change_from_outside(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let change = net.change("n1", Some("before"), "outside", &STRANGER)?;
    // Sealed with the mesh's key, so it reaches the signature check.
    net.relay.inject(&change, &KEY, net.port(0)?)
}

#[then("the row is unchanged")]
fn the_row_is_unchanged(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    sleep(Duration::from_secs(2));
    match net.body(0, "n1")?.as_deref() {
        Some("before") => Ok(()),
        other => Err(format!("the row holds {other:?}")),
    }
}

#[given(
    "a node that holds a newer key and the previous key, whose last change's sunset has not passed"
)]
fn a_node_with_two_keys_and_a_live_sunset(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.start(
        &[PREVIOUS, KEY],
        GRAVEYARD,
        "INSERT INTO notes VALUES ('n1', 'before')",
    )?;
    sleep(Duration::from_secs(1));
    Ok(())
}

#[when("it receives a change signed with the previous key")]
fn a_change_under_the_previous_key(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let change = net.change("n1", Some("before"), "previous", &PREVIOUS)?;
    net.relay.inject(&change, &PREVIOUS, net.port(0)?)
}

#[then("the change is applied")]
fn the_change_is_applied(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.holds(0, "n1", "previous", SOON)
}

#[given("a node holding the previous key and a newer key")]
fn a_node_with_two_keys(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.start(&[PREVIOUS, KEY], GRAVEYARD, "")?;
    sleep(Duration::from_secs(1));
    Ok(())
}

#[when("it sends a change")]
fn it_sends_a_change(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.sql(0, "INSERT INTO notes VALUES ('n9', 'signed')")?;
    wait(SOON, || !net.sent_by(0).unwrap_or_default().is_empty())
        .map_err(|()| "the node sent no change".to_owned())
}

#[then("the change's key_id names the newer key")]
fn named_by_the_newer_key(mesh: &Mesh) -> Result<(), String> {
    let sent = mesh.net()?.sent_by(0)?;
    let (_, envelope) = sent.first().ok_or("no change was sent")?;
    if envelope.key_id == key_id(&KEY) && envelope.verify(&signing(&KEY)) {
        Ok(())
    } else {
        Err(format!(
            "the change names key {}, the newer key is {}",
            envelope.key_id,
            key_id(&KEY)
        ))
    }
}

#[given("three nodes running smugglr mesh, and the first node gone")]
fn three_nodes_and_the_first_gone(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.start_many(3, "")?;
    if let Some(first) = net.nodes().first_mut() {
        first.stop();
    }
    Ok(())
}

#[when("a row is written on the second node's database")]
fn a_row_on_the_second(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?
        .sql(1, "INSERT INTO notes VALUES ('n9', 'without the first')")
}

#[then("the third node holds that row")]
fn the_third_holds_that_row(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.holds(2, "n9", "without the first", SOON)
}

/// The known value and key the capture is searched for.
const SECRET_VALUE: &str = "smuggled-cargo-7f3a91";
const SECRET_KEY: &str = "key-5e02c4";

#[given("two mesh nodes sharing a key, and a capture of the traffic between them")]
fn two_nodes_captured(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.start_many(2, "")
}

#[when("a row holding a known value is changed on one node")]
fn a_known_value_changes(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.sql(
        0,
        &format!("INSERT INTO notes VALUES ('{SECRET_KEY}', '{SECRET_VALUE}')"),
    )?;
    net.holds(1, SECRET_KEY, SECRET_VALUE, SOON)
}

#[then("the capture holds neither that value nor that row's key in the clear")]
fn nothing_in_the_clear(mesh: &Mesh) -> Result<(), String> {
    let seen = mesh.net()?.relay.seen();
    if !seen.iter().any(|seen| changed(seen).is_some()) {
        return Err("the capture holds no change".to_owned());
    }
    for needle in [SECRET_VALUE, SECRET_KEY] {
        let found = seen.iter().any(|seen| {
            seen.bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes())
        });
        if found {
            return Err(format!("the capture holds {needle:?} in the clear"));
        }
    }
    Ok(())
}

impl Net {
    fn stop(&self, index: usize) -> Result<(), String> {
        let mut nodes = self.nodes();
        nodes
            .get_mut(index)
            .ok_or_else(|| format!("no node {index}"))?
            .stop();
        Ok(())
    }

    /// Starts again node `index`, stopped, on its database, config and port.
    fn restart(&self, index: usize) -> Result<(), String> {
        let mut nodes = self.nodes();
        nodes
            .get_mut(index)
            .ok_or_else(|| format!("no node {index}"))?
            .run();
        drop(nodes);
        // Its look at its rows is done well within this.
        sleep(Duration::from_secs(1));
        Ok(())
    }

    fn count(&self, index: usize) -> Result<i64, String> {
        let db = Connection::open(self.path(index)?).map_err(|err| err.to_string())?;
        db.busy_timeout(Duration::from_secs(5))
            .map_err(|err| err.to_string())?;
        db.query_row("SELECT count(*) FROM notes", [], |row| row.get(0))
            .map_err(|err| err.to_string())
    }
}

#[given("two nodes running smugglr mesh, and the second node stopped")]
fn two_nodes_the_second_stopped(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.start_many(2, "")?;
    net.stop(1)
}

#[when("a row is written on the first node, and the first node is stopped and started again")]
fn the_first_restarts(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.sql(0, "INSERT INTO notes VALUES ('n9', 'kept')")?;
    // The row is in the outbox once the node has sent it.
    wait(SOON, || {
        net.sent_by(0)
            .unwrap_or_default()
            .iter()
            .any(|(_, envelope)| envelope.change.key == ["'n9'"])
    })
    .map_err(|()| "the first node never sent the row".to_owned())?;
    net.stop(0)?;
    net.restart(0)
}

#[when("the second node starts again")]
fn the_second_starts_again(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.restart(1)
}

#[then("the second node holds the row")]
fn the_second_holds_the_row(mesh: &Mesh) -> Result<(), String> {
    // The first node resends at 10, 20 and 40 s after it starts again.
    mesh.net()?.holds(1, "n9", "kept", Duration::from_secs(80))
}

#[then("the user's database holds no table smugglr made")]
fn no_table_of_smugglrs(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    for index in 0..2 {
        let db = Connection::open(net.path(index)?).map_err(|err| err.to_string())?;
        let mut statement = db
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table'")
            .map_err(|err| err.to_string())?;
        let tables: Vec<String> = statement
            .query_map([], |row| row.get(0))
            .map_err(|err| err.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|err| err.to_string())?;
        if tables != ["notes"] {
            return Err(format!("node {index}'s database holds {tables:?}"));
        }
    }
    Ok(())
}

/// The backpressure line the scenario sets: 1 MB.
const LINE: u64 = 1_000_000;

/// How many rows make the 5 MB written: each row's body is 1,000 bytes.
const ROWS: i64 = 5_000;

#[given("a node with backpressure set to 1 MB whose only peer is asleep")]
fn a_node_with_a_line(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.start_with(&[KEY], GRAVEYARD, LINE, "")?;
    net.start(&[KEY], GRAVEYARD, "")?;
    // Each hears the other before the peer sleeps.
    net.sql(0, "INSERT INTO notes VALUES ('w0', 'heard')")?;
    net.holds(1, "w0", "heard", SOON)?;
    sleep(Duration::from_secs(1));
    net.stop(1)
}

#[when("5 MB of rows are written on the node")]
fn five_mb_written(mesh: &Mesh) -> Result<(), String> {
    mesh.net()?.sql(
        0,
        &format!(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {ROWS}) \
             INSERT INTO notes SELECT 'r' || i, printf('%.1000c', 'x') FROM n"
        ),
    )
}

#[then("the node's outbox holds no more than 1 MB")]
fn the_outbox_holds_its_line(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    let outbox = || net.nodes().first().map_or(0, Node::outbox);
    // Several looks for its own changes.
    wait(SOON, || outbox() > LINE / 2)
        .map_err(|()| format!("the outbox only reached {} bytes", outbox()))?;
    sleep(Duration::from_secs(3));
    match outbox() {
        bytes if bytes <= LINE => Ok(()),
        bytes => Err(format!("the outbox holds {bytes} bytes")),
    }
}

#[then(
    "once the peer wakes and acknowledges, the node takes the remaining rows and the peer ends with them all"
)]
fn the_peer_ends_with_all(mesh: &Mesh) -> Result<(), String> {
    let net = mesh.net()?;
    net.restart(1)?;
    wait(Duration::from_secs(180), || {
        net.count(1).is_ok_and(|count| count == ROWS + 1)
    })
    .map_err(|()| format!("the peer holds {:?} rows of {}", net.count(1), ROWS + 1))
}

/// A delete made on one node after its insert crossed reaches the other,
/// its first copy lost on the way so only a resend can bring it, and the
/// node records that the change arrived only as a resend.
#[test]
fn a_delete_crosses_after_its_insert() -> Result<(), String> {
    let net = Net::new()?;
    net.start_many(2, "")?;
    net.sql(0, "INSERT INTO notes VALUES ('p2', 'made')")?;
    net.holds(1, "p2", "made", SOON)?;
    let maker = net.port(0)?;
    let filter: Filter = Box::new(move |seen, before| {
        let delete = |seen: &Seen| {
            changed(seen).is_some_and(|envelope| {
                envelope.change.key == ["'p2'"] && envelope.change.to.is_none()
            })
        };
        delete(seen)
            && seen.from == maker
            && !before
                .iter()
                .any(|other| other.from == maker && delete(other))
    });
    *net.relay
        .filter
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(filter);
    net.sql(0, "DELETE FROM notes WHERE id = 'p2'")?;
    wait(Duration::from_secs(20), || {
        net.body(1, "p2").is_ok_and(|body| body.is_none())
    })
    .map_err(|()| format!("node 1 still holds p2: {:?}", net.body(1, "p2")))
}
