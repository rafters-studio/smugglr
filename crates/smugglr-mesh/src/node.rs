//! One node: finds its own changes and sends them, paced; applies the
//! changes it hears in batches, acknowledges and echoes them; and resends
//! its own until every peer has them or their sunset passes.
//!
//! Broadcast finds peers and unicast talks to them (FR-SMUGGLR-078 rev
//! 19): a node announces itself by broadcast, and to the addresses in its
//! peers setting, when it starts and every [`ANNOUNCE`]; a node that hears
//! a peer for the first time announces itself back to it. A change's first
//! send, the maker's resends and every acknowledgement go unicast to each
//! peer's last-seen address (the source of its datagrams, never its name),
//! because a link retries a unicast frame and does not retry a broadcast
//! one; echoes are broadcast. A peer on this same machine and port shares
//! the node's socket group, where a unicast reaches only one socket, so it
//! is sent to by broadcast.
//!
//! A reader thread drains the socket into memory as datagrams arrive, so a
//! long look at the database or a large batch never leaves the socket's
//! buffer to overflow. Everything a node sends is paced: first sends are
//! held to a window that starts small, grows with each change a peer
//! acknowledges and halves only on congestion, a full send buffer or
//! acknowledgements slowing, never for a lost broadcast (RFC 8085), and
//! each tick sends at most [`PER_TICK`] datagrams, resends and echoes
//! first.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::OsRng;
use chacha20poly1305::aead::rand_core::RngCore;
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::{hex, unhex};
use smugglr_core::row_hash::row_hash;
use smugglr_place::Place;
use socket2::{Domain, Protocol, Socket, Type};

use crate::local::{Local, limit, refuse};
use crate::named::{keep_resolved, literal, resolve_now};
use crate::outbox::{Acked, Outbox, Timing};
use crate::parts::{Piece, Receiving, Sending};
use crate::wazzup::{ASK_AGAIN, Asked, Asking, Settled, named_line, settle};
use crate::wire::{
    ACKS, DATAGRAM, Id, Line, Message, Range, RowNow, decode, encode, open, seal, sealed_len,
};
use crate::{CHUNK, Error, Settings, sealing, signing};

/// How long a node waits for a datagram before it looks at its clocks.
const TICK: Duration = Duration::from_millis(100);

/// The shortest wait between two looks for its own changes; a look that
/// took long waits twice as long as it took.
const SCAN: Duration = Duration::from_millis(500);

/// The most datagrams a node sends in one tick.
const PER_TICK: usize = 2_000;

/// The window a node's first sends start from.
const FIRST_WINDOW: f64 = 32.0;

/// The largest window. A larger one queues more first copies at a busy
/// peer, and a resend waits behind them all: past about 1,000, a lost copy
/// takes longer than a second to repair.
const LARGEST: f64 = 1_000.0;

/// How often a node announces itself again, so a peer that missed it, or
/// started since, learns its address.
const ANNOUNCE: Duration = Duration::from_secs(10);

/// A peer heard within this long is awake: three of its announcements.
const HEARD_WITHIN: Duration = Duration::from_secs(30);

/// A peer that owes acknowledgements and has given none for this long is
/// named in the record.
const SILENCE: Duration = Duration::from_secs(5 * 60);

/// How often the record names a peer while its silence lasts.
const SAY_SILENCE: Duration = Duration::from_secs(10 * 60);

/// How long a node that has just started listens for its name being taken,
/// before it joins.
const JOIN: Duration = Duration::from_secs(1);

/// How often a starting node says its wazzup, so one lost copy costs nothing.
const WAZZUP_EVERY: Duration = Duration::from_millis(300);

/// The id of the wazzups a node says while it listens for its name being
/// taken, which ask after nothing else.
const NAME_ONLY: u64 = 0;

/// How long this node remembers a change it sent, to tell its own from a
/// change under its name that another machine made.
const MADE_FOR: Duration = Duration::from_secs(60);

/// How often at most the record names resends, or datagrams that did not
/// open.
const SAY_EVERY: Duration = Duration::from_secs(60);

/// How often the outbox and what the node has heard are tidied.
const TIDY: Duration = Duration::from_secs(1);

/// Bytes kept free under the backpressure line for the outbox's index.
const INDEX_ROOM: u64 = 1_024;

/// The most applied changes waiting for their echo.
const ECHOES: usize = 20_000;

/// The most echoes a tick sends.
const ECHOES_PER_TICK: usize = 200;

/// The resends a tick may send when the node has nothing left to send
/// first.
const IDLE_RESENDS: usize = 100;

/// The resends a tick may send when its window is held by changes waiting
/// on repair.
const BLOCKED_RESENDS: usize = 10;

/// How often the counts behind the resend quarter halve, so it is the
/// quarter of what the node has sent lately.
const COUNTED: Duration = Duration::from_secs(60);

/// Why the window halves when a send finds the send buffer full.
const FULL: &str = "the send buffer is full";

/// A round trip shorter than this is never acknowledgements slowing.
const SLOWING: Duration = Duration::from_millis(250);

/// The receive buffer a node asks its system for.
const RECEIVE_BUFFER: usize = 8 << 20;

/// Replays the change the node's dropped log holds under `change`, its
/// change hash as hex (FR-SMUGGLR-076): re-sends it with a fresh sunset,
/// overriding its drop, decides it on the node's own database through the
/// one decision path, prints what came of it, and returns whether it was
/// decided: `false` when it waits for its `from`, and so stays in the log.
///
/// # Errors
/// [`Error::NotDropped`] when the dropped log holds no such change; when the
/// node holds no key; or when its place or what smugglr keeps for it cannot
/// be read or written.
pub fn replay(place: &mut dyn Place, settings: &Settings, change: &str) -> Result<bool, Error> {
    let newest = settings.keys.last().ok_or(Error::NoKey)?;
    let not_dropped = || Error::NotDropped(change.to_owned());
    let hash: [u8; 32] = unhex(change)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(not_dropped)?;
    let folder = folder(place);
    let mut local = Local::open(place, settings, &folder, now())?;
    let applied = local
        .replay_dropped(place, settings, &signing(newest), &hash, now())?
        .ok_or_else(not_dropped)?;
    if !applied.waiting.is_empty() {
        println!("smugglr mesh: change {change} waits for its from; it stays in the dropped log");
        return Ok(false);
    }
    println!("smugglr mesh: replayed change {change}");
    Ok(true)
}

/// Where smugglr keeps what the node whose database is `place` needs.
fn folder(place: &dyn Place) -> String {
    format!("mesh/{}", hex(&row_hash(&["mesh", place.address()])))
}

/// Tells the mesh that `node` is gone for good: peers stop owing it changes.
/// Said a few times, since a datagram can be lost and a peer's first hearing
/// passes it on.
///
/// # Errors
/// When the mesh holds no key, or no socket can be opened to say it from.
pub fn remove(settings: &Settings, node: &str) -> Result<(), Error> {
    let key = settings.keys.last().ok_or(Error::NoKey)?;
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.set_broadcast(true)?;
    let datagram = seal(
        &Message::Remove {
            sender: "smugglr mesh remove".to_owned(),
            node: node.to_owned(),
        },
        &sealing(key),
    );
    let mut to = vec![
        SocketAddr::from(([255, 255, 255, 255], settings.port)),
        SocketAddr::from(([127, 0, 0, 1], settings.port)),
    ];
    to.extend(resolve_now(&settings.peers));
    for _ in 0..3 {
        for to in &to {
            // A network that carries no broadcast refuses that send, and the
            // peers named here are told anyway.
            let _ = socket.send_to(&datagram, to);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(())
}

/// Runs the node whose database is `place` until `stop` is set.
///
/// # Errors
/// When the node holds no key, its socket cannot be opened, or its place or
/// what smugglr keeps for it cannot be read or written.
pub fn run(place: &mut dyn Place, settings: &Settings, stop: &AtomicBool) -> Result<(), Error> {
    // The newest key signs and seals; the one before it is still accepted.
    let held = &settings.keys[settings.keys.len().saturating_sub(2)..];
    if held.is_empty() {
        return Err(Error::NoKey);
    }
    let socket = bind(settings.port)?;
    let reader = socket.try_clone()?;
    let named = Arc::new(Mutex::new(literal(&settings.peers)));
    let folder = folder(place);
    let ran = format!("{folder}/ran");
    let since = last_ran(settings, &ran)?;
    let past_sunset = say_if_past_sunset(settings, &ran, place.address())?;
    let removed_file = format!("{folder}/removed.json");
    let removed = removed_nodes(settings, &removed_file)?;
    let local = Local::open(place, settings, &folder, now())?;
    let outbox = open_outbox(settings, &folder, &removed)?;
    let mut node = Node {
        settings,
        socket,
        to: SocketAddr::from(([255, 255, 255, 255], settings.port)),
        seal: held.iter().rev().map(sealing).collect(),
        sign: held.iter().rev().map(signing).collect(),
        local,
        outbox,
        sending: Sending::default(),
        receiving: Receiving::new(limit(place, settings).0),
        peers: BTreeSet::new(),
        timing: HashMap::new(),
        sent: std::cell::Cell::new(0),
        resent: std::cell::Cell::new(0),
        counted: Instant::now(),
        addresses: HashMap::new(),
        heard_at: HashMap::new(),
        start: OsRng.next_u64(),
        joining_until: Instant::now() + JOIN,
        said_wazzup: None,
        arrived_joining: Vec::new(),
        since,
        asking: Vec::new(),
        last_ask: 0,
        ask_on_joining: !past_sunset,
        wazzups: Vec::new(),
        answers: Vec::new(),
        woken_by: HashMap::new(),
        told_too_much: false,
        removed,
        removed_file,
        clash: None,
        made: HashMap::new(),
        named: named.clone(),
        silent: HashMap::new(),
        keys: HashMap::new(),
        own: own_address(settings.port).into_iter().collect(),
        heard: HashMap::new(),
        waiting: Vec::new(),
        echoes: VecDeque::new(),
        window: Window::new(),
        scanned: None,
        scan_took: Duration::ZERO,
        scan_full: false,
        lost: 0,
        announced: None,
        ran,
        resent_unsaid: 0,
        said_resent: None,
        unopened: 0,
        said_unopened: None,
        tidied: Instant::now(),
    };
    let (tx, rx) = mpsc::channel();
    let reading = AtomicBool::new(true);
    let resolving = Arc::new(AtomicBool::new(true));
    if !settings.peers.is_empty() {
        keep_resolved(settings.peers.clone(), named, resolving.clone(), ANNOUNCE);
    }
    let mut ended = Ok(());
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut buffer = vec![0; 65_536];
            while reading.load(Ordering::Relaxed) {
                if let Ok((len, from)) = reader.recv_from(&mut buffer)
                    && tx.send((buffer[..len].to_vec(), from)).is_err()
                {
                    break;
                }
            }
        });
        while !stop.load(Ordering::Relaxed) {
            // A busy database or a datagram that could not go out is a
            // moment's trouble: the node says so and goes on. A look that
            // failed kept nothing, so its rows are found again, and a change
            // that did not go out is resent.
            match node.turn(place, &rx) {
                Ok(()) => {}
                Err(err @ (Error::NameTaken(_) | Error::NameClash(_))) => {
                    ended = Err(err);
                    break;
                }
                Err(err) => eprintln!("smugglr mesh: {err}"),
            }
        }
        reading.store(false, Ordering::Relaxed);
        resolving.store(false, Ordering::Relaxed);
    });
    ended
}

/// A UDP socket on `port` that several nodes on one machine can share, and
/// that may broadcast.
fn bind(port: u16) -> std::io::Result<UdpSocket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    socket.set_broadcast(true)?;
    // The largest receive buffer the system allows, up to RECEIVE_BUFFER;
    // a system that refuses one too large keeps its default.
    let mut size = RECEIVE_BUFFER;
    while size >= 1 << 20 && socket.set_recv_buffer_size(size).is_err() {
        size /= 2;
    }
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(Some(TICK))?;
    Ok(socket)
}

/// Whether this node comes back past the sunset; it says so, and how to catch
/// up.
fn say_if_past_sunset(settings: &Settings, ran: &str, place: &str) -> Result<bool, Error> {
    let Some(away) = away(settings, ran)? else {
        return Ok(false);
    };
    println!("{}", catch_up(away, settings, place));
    Ok(true)
}

/// The node's outbox, owing nothing to a node taken off the mesh: each
/// segment keeps the targets it was written with.
fn open_outbox(
    settings: &Settings,
    folder: &str,
    removed: &BTreeSet<String>,
) -> Result<Outbox, Error> {
    let mut outbox = Outbox::open(settings.store.clone(), &format!("{folder}/outbox"), now())?;
    for node in removed {
        outbox.remove_target(node);
    }
    Ok(outbox)
}

/// When the node last ran, in seconds since the epoch; 0 when it never has.
fn last_ran(settings: &Settings, ran: &str) -> Result<u64, Error> {
    Ok(settings
        .store
        .read(ran)?
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or_default())
}

/// The nodes a person has taken off the mesh.
fn removed_nodes(settings: &Settings, file: &str) -> Result<BTreeSet<String>, Error> {
    Ok(settings
        .store
        .read(file)?
        .and_then(|text| serde_json::from_str(&text).ok())
        .unwrap_or_default())
}

/// One part of a peer's wazzup, to answer.
#[derive(Debug)]
struct Wazzup {
    peer: String,
    start: u64,
    id: u64,
    n: u64,
    count: u64,
    lines: Vec<Line>,
}

/// A peer's answer to a part of this node's wazzup.
#[derive(Debug)]
struct Answer {
    peer: String,
    id: u64,
    n: u64,
    too_much: bool,
    rows: Vec<RowNow>,
}

/// Whether the record should name a peer silent for `quiet`, last named
/// `said` ago.
fn silence_due(quiet: Duration, said: Option<Duration>) -> bool {
    quiet >= SILENCE && said.is_none_or(|said| said >= SAY_SILENCE)
}

/// The record's line for a silent peer. `heard` is how long ago it last sent
/// anything: a peer still heard is holding changes, one not heard is away.
fn silence_line(peer: &str, quiet: Duration, count: usize, heard: Option<Duration>) -> String {
    let changes = if count == 1 { "change" } else { "changes" };
    let heard = heard.map_or_else(
        || "not heard since this node started".to_owned(),
        |heard| format!("last heard {} ago", span(heard.as_secs())),
    );
    format!(
        "smugglr mesh: {peer} has acknowledged nothing for {}; {count} {changes} owed to it, {heard}",
        span(quiet.as_secs())
    )
}

/// A change this node has heard.
#[derive(Debug)]
struct Heard {
    /// The node has the change: applied, or already at it.
    has: bool,
    /// Copies heard since the node had it.
    copies: u32,
    sunset: u64,
}

/// How many first sends may be unacknowledged at once (RFC 8085).
#[derive(Debug)]
struct Window {
    size: f64,
    threshold: f64,
    cut: Option<Instant>,
}

impl Window {
    fn new() -> Self {
        Self {
            size: FIRST_WINDOW,
            threshold: LARGEST,
            cut: None,
        }
    }

    /// A first acknowledgement of one change: the window grows.
    fn acked(&mut self) {
        self.size += if self.size < self.threshold {
            1.0
        } else {
            1.0 / self.size
        };
        self.size = self.size.min(LARGEST);
    }

    /// Congestion, `why`: a full send buffer or acknowledgements slowing.
    /// The window halves, at most once a second, and the record says so. A
    /// lost broadcast never shrinks it (FR-SMUGGLR-078 rev 17).
    fn congested(&mut self, why: &str) {
        if self
            .cut
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(1))
        {
            self.threshold = (self.size / 2.0).max(4.0);
            self.size = self.threshold;
            self.cut = Some(Instant::now());
            println!(
                "smugglr mesh: send window halved to {}: {why}",
                self.whole()
            );
        }
    }

    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "the window is between 4 and LARGEST"
    )]
    fn whole(&self) -> usize {
        self.size as usize
    }
}

struct Node<'a> {
    settings: &'a Settings,
    socket: UdpSocket,
    /// Where the node sends: the broadcast address on its port.
    to: SocketAddr,
    /// The sealing keys it opens with, newest first; it seals with the first.
    seal: Vec<[u8; 32]>,
    /// The signing keys it accepts, newest first; it signs with the first.
    sign: Vec<[u8; 32]>,
    local: Local,
    outbox: Outbox,
    /// Its own changes it sends in parts.
    sending: Sending,
    /// The changes it assembles from their parts.
    receiving: Receiving,
    /// The nodes it has heard.
    peers: BTreeSet<String>,
    /// How each peer has been acknowledging this node's changes.
    timing: HashMap<String, Timing>,
    /// Datagrams sent lately, and of them resends.
    sent: std::cell::Cell<u64>,
    resent: std::cell::Cell<u64>,
    counted: Instant,
    /// Each peer's address, as its last datagram came from.
    addresses: HashMap<String, SocketAddr>,
    /// When each peer was last heard, by anything it sent.
    heard_at: HashMap<String, Instant>,
    /// The random number made at this start, which tells this node from
    /// another of its name.
    start: u64,
    /// Until when it only listens for its name being taken.
    joining_until: Instant,
    said_wazzup: Option<Instant>,
    /// What arrived while it listened, to apply once it joins.
    arrived_joining: Vec<(String, Envelope)>,
    /// When it last ran before this start, in seconds since the epoch.
    since: u64,
    /// The wazzups under way, and the id of the last one begun.
    asking: Vec<Asking>,
    last_ask: u64,
    /// It still owes its first wazzup. A node back past the sunset does not
    /// ask: its changes are dead (toy X), and it catches up by stash first.
    ask_on_joining: bool,
    /// Parts of peers' wazzups to answer, and answers to its own, to take in
    /// where the database can be read.
    wazzups: Vec<Wazzup>,
    answers: Vec<Answer>,
    /// The wazzup each peer last woke this node's resends with: its start,
    /// since ids begin again at each start, and its id.
    woken_by: HashMap<String, (u64, u64)>,
    told_too_much: bool,
    /// The nodes a person has taken off the mesh.
    removed: BTreeSet<String>,
    removed_file: String,
    /// Why this node must stop: its name belongs to another.
    clash: Option<Error>,
    /// The changes it sent lately, which may yet come back to it.
    made: HashMap<[u8; 32], Instant>,
    /// What the peers setting's names resolve to now.
    named: Arc<Mutex<Vec<SocketAddr>>>,
    /// Each peer the record has named as silent: since when, and when it
    /// last said so.
    silent: HashMap<String, (Instant, Instant)>,
    /// Which sealing key opened each peer's last datagram.
    keys: HashMap<String, usize>,
    /// This machine's addresses, as the node's own broadcasts come from.
    own: BTreeSet<IpAddr>,
    heard: HashMap<[u8; 32], Heard>,
    /// Changes that wait for their `from` to appear.
    waiting: Vec<Envelope>,
    /// Applied changes due an echo, in the order they are due.
    echoes: VecDeque<(Instant, Envelope)>,
    window: Window,
    scanned: Option<Instant>,
    scan_took: Duration,
    /// The last look stopped at the backpressure line.
    scan_full: bool,
    /// Changes heard this turn only as a resend.
    lost: usize,
    /// When the node last announced itself.
    announced: Option<Instant>,
    /// Where it keeps when it last ran.
    ran: String,
    /// Changes resent since the record last said so.
    resent_unsaid: usize,
    said_resent: Option<Instant>,
    /// Datagrams that opened under no key held, since the record last said.
    unopened: usize,
    said_unopened: Option<Instant>,
    tidied: Instant,
}

impl Node<'_> {
    /// Takes in what has arrived, up to a batch, applies it, looks for its
    /// own changes when a look is due, and sends what is due.
    fn turn(
        &mut self,
        place: &mut dyn Place,
        rx: &Receiver<(Vec<u8>, SocketAddr)>,
    ) -> Result<(), Error> {
        let mut batch: Vec<(String, Envelope)> = Vec::new();
        let mut first = match rx.recv_timeout(TICK) {
            Ok(datagram) => Some(datagram),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        while let Some((datagram, from)) = first.take().or_else(|| rx.try_recv().ok()) {
            match open(&datagram, &self.seal) {
                Some((message, key)) => {
                    // A peer is spoken to in the key it speaks, so a peer
                    // that holds only the previous key still hears this one.
                    if message.sender() != self.settings.node {
                        self.keys.insert(message.sender().to_owned(), key);
                    }
                    self.hear(message, from, &mut batch);
                }
                None => self.unopened += 1,
            }
            if batch.len() >= CHUNK {
                break;
            }
        }
        if let Some(clash) = self.clash.take() {
            return Err(clash);
        }
        if Instant::now() < self.joining_until {
            // Nothing is applied or sent until the name proves free. What
            // arrived now waits for that: dropped, its maker would resend it
            // only on the slow interval for a peer it has not heard answer.
            self.arrived_joining.append(&mut batch);
            self.say_wazzup();
            return Ok(());
        }
        batch.append(&mut self.arrived_joining);
        if !batch.is_empty() {
            self.apply(place, batch)?;
        }
        self.joined(place)?;
        for (maker, ack) in self.receiving.acks(&self.settings.node) {
            // A lost acknowledgement of parts is covered by the maker's
            // resend and the next acknowledgement's missing parts.
            let _ = self.send(&ack, self.address(&maker));
        }
        if self.lost > 0 {
            println!(
                "smugglr mesh: {} arrived only as a resend; the first copy was lost",
                if self.lost == 1 {
                    "1 change".to_owned()
                } else {
                    format!("{} changes", self.lost)
                }
            );
            self.lost = 0;
        }
        if self.outbox.settled() {
            // Room in the outbox as soon as acknowledgements make it.
            self.outbox.prune(now())?;
        }
        self.look(place)?;
        self.send_due()?;
        if self.unopened > 0
            && self
                .said_unopened
                .is_none_or(|at| at.elapsed() >= SAY_EVERY)
        {
            // Another mesh on the port, or a peer holding another key.
            println!(
                "smugglr mesh: {} on port {} did not open under a key this node holds",
                if self.unopened == 1 {
                    "1 datagram".to_owned()
                } else {
                    format!("{} datagrams", self.unopened)
                },
                self.settings.port
            );
            self.unopened = 0;
            self.said_unopened = Some(Instant::now());
        }
        if self.announced.is_none_or(|at| at.elapsed() >= ANNOUNCE) {
            self.say_silence(Instant::now())?;
            self.announce();
        }
        if self.tidied.elapsed() >= TIDY {
            self.tidied = Instant::now();
            let epoch = now();
            self.outbox.prune(epoch)?;
            self.receiving.tidy();
            self.heard.retain(|_, heard| epoch <= heard.sunset);
            self.made.retain(|_, at| at.elapsed() < MADE_FOR);
            if !self.waiting.is_empty() {
                // A change waiting past its sunset is dropped.
                self.apply(place, Vec::new())?;
            }
        }
        Ok(())
    }

    /// Looks for its own changes when a look is due, and takes what fits
    /// under the backpressure line into the outbox.
    fn look(&mut self, place: &mut dyn Place) -> Result<(), Error> {
        // A look that stopped at the backpressure line looks again once
        // acknowledgements have emptied half the outbox, not for every
        // change they settle; any other look waits its turn.
        let due = if self.scan_full {
            // Or once fewer changes wait for their first send than the
            // window holds, so first sends never run dry while acks drain:
            // a dry spell leaves the last changes sent nothing after them
            // to show a gap, and their repair waits on round trips.
            self.outbox.bytes() <= self.settings.backpressure / 2
                || (self.outbox.bytes() < self.settings.backpressure
                    && self.outbox.unsent() < self.window.whole())
        } else {
            self.outbox.bytes() < self.settings.backpressure
                && self
                    .scanned
                    .is_none_or(|at| at.elapsed() >= SCAN.max(self.scan_took * 2))
        };
        if due {
            let started = Instant::now();
            // Room for the segment index to grow by the segments added.
            let budget = self
                .settings
                .backpressure
                .saturating_sub(self.outbox.bytes() + INDEX_ROOM);
            let peers = &self.peers;
            let found = self.local.changes(
                place,
                self.settings,
                &self.sign[0],
                now(),
                budget,
                |envelope| Outbox::weigh(envelope, peers),
            )?;
            self.scan_full = found.full;
            if !found.found.is_empty() {
                // In the outbox first, then kept as each key's last hash:
                // a step that fails leaves the rows to be found again.
                self.outbox.add(&found.found, &self.peers)?;
                self.local.keep(&found.found)?;
            }
            self.scanned = Some(Instant::now());
            self.scan_took = started.elapsed();
        }
        Ok(())
    }

    fn send(&self, message: &Message, (to, key): (SocketAddr, usize)) -> std::io::Result<()> {
        self.socket
            .send_to(&seal(message, &self.seal[key]), to)
            .map(drop)?;
        self.sent.set(self.sent.get() + 1);
        Ok(())
    }

    /// How many resend datagrams may go now, so resends never take more
    /// than a quarter of what the node sends (FR-SMUGGLR-078 rev 20).
    fn resend_room(&self) -> usize {
        let (sent, resent) = (self.sent.get(), self.resent.get());
        let others = sent.saturating_sub(resent);
        usize::try_from((others / 3).saturating_sub(resent)).unwrap_or(usize::MAX)
    }

    fn change(
        &self,
        envelope: &Envelope,
        resend: bool,
        to: (SocketAddr, usize),
    ) -> std::io::Result<()> {
        self.send(
            &Message::Change {
                sender: self.settings.node.clone(),
                envelope: Box::new(envelope.clone()),
                resend,
            },
            to,
        )
    }

    /// Where a message for `peer` alone goes: its last-seen address, or the
    /// broadcast address for a peer in this node's own socket group or one
    /// whose address is not known.
    fn address(&self, peer: &str) -> (SocketAddr, usize) {
        let address =
            unicast(self.addresses.get(peer), &self.own, self.settings.port).unwrap_or(self.to);
        (address, self.keys.get(peer).copied().unwrap_or(0))
    }

    /// Where a change's first send goes: each known peer's address, once
    /// each; a node that knows no peer yet says it to whoever can hear.
    fn destinations(&self) -> BTreeSet<(SocketAddr, usize)> {
        if self.peers.is_empty() {
            return std::iter::once(self.to)
                .chain(self.named_addresses())
                .map(|to| (to, 0))
                .collect();
        }
        self.peers.iter().map(|peer| self.address(peer)).collect()
    }

    /// Says the node is here: by broadcast, and to each address in the
    /// peers setting.
    fn announce(&mut self) {
        self.announced = Some(Instant::now());
        // When the node last ran, for the next start to know how long it
        // was away; a write that fails only costs that message.
        let _ = self.settings.store.replace(&self.ran, &now().to_string());
        let announce = Message::Announce {
            sender: self.settings.node.clone(),
        };
        // A lost announcement is said again at the next.
        let _ = self.send(&announce, (self.to, 0));
        for peer in self.named_addresses() {
            let _ = self.send(&announce, (peer, 0));
        }
    }

    /// A message under this node's own name: its own broadcast come back, or
    /// another machine of that name.
    fn hear_own_name(&mut self, message: &Message, from: SocketAddr) {
        let name = &self.settings.node;
        match message {
            // Only a node that has joined claims the name, so two starting
            // at once do not both give way.
            Message::Wazzup { start, .. }
                if *start != self.start && Instant::now() >= self.joining_until =>
            {
                let taken = Message::NameTaken {
                    sender: name.clone(),
                    start: *start,
                };
                let _ = self.send(&taken, (from, 0));
            }
            Message::NameTaken { start, .. } if *start == self.start => {
                self.clash = Some(Error::NameTaken(name.clone()));
            }
            Message::Change { envelope, .. }
                if envelope.node == *name
                    && !self.outbox.holds(&envelope.signature)
                    && !self.made.contains_key(&envelope.signature) =>
            {
                self.clash = Some(Error::NameClash(name.clone()));
            }
            // The node hears its own broadcasts: from its own addresses.
            _ => {
                self.own.insert(from.ip());
            }
        }
    }

    /// Says the first wazzup of this start, at most every [`WAZZUP_EVERY`].
    fn say_wazzup(&mut self) {
        if self
            .said_wazzup
            .is_some_and(|at| at.elapsed() < WAZZUP_EVERY)
        {
            return;
        }
        self.said_wazzup = Some(Instant::now());
        let wazzup = Message::Wazzup {
            sender: self.settings.node.clone(),
            start: self.start,
            since: self.since,
            id: NAME_ONLY,
            n: 0,
            count: 1,
            lines: Vec::new(),
        };
        // A lost wazzup is said again.
        let _ = self.send(&wazzup, (self.to, 0));
        for peer in self.named_addresses() {
            let _ = self.send(&wazzup, (peer, 0));
        }
    }

    /// The wazzup's work, on a node that has joined: its own first wazzup,
    /// what peers asked, what they answered, and parts still unanswered.
    fn joined(&mut self, place: &mut dyn Place) -> Result<(), Error> {
        for (peer, named) in self.outbox.take_expired() {
            println!("{}", named_line(&peer, &named));
        }
        if self.ask_on_joining {
            self.ask_on_joining = false;
            self.ask(None)?;
        }
        for wazzup in std::mem::take(&mut self.wazzups) {
            self.answer_wazzup(place, wazzup)?;
        }
        for answer in std::mem::take(&mut self.answers) {
            self.take_answer(place, answer)?;
        }
        self.say_parts();
        Ok(())
    }

    /// Begins a wazzup about the changes owed: to `only`, or to every peer.
    fn ask(&mut self, only: Option<String>) -> Result<(), Error> {
        let asked = self
            .outbox
            .owed_changes(only.as_deref())?
            .iter()
            .map(Asked::of)
            .collect();
        self.last_ask += 1;
        self.asking.push(Asking::new(self.last_ask, asked, only));
        Ok(())
    }

    /// Says each part a peer it asks has not answered, at most every
    /// [`ASK_AGAIN`]; a wazzup ends once no peer that is still heard owes an
    /// answer.
    fn say_parts(&mut self) {
        let mut asking = std::mem::take(&mut self.asking);
        asking.retain_mut(|wazzup| {
            if wazzup.last_said.is_some_and(|at| at.elapsed() < ASK_AGAIN) {
                return true;
            }
            let first = wazzup.last_said.is_none();
            wazzup.last_said = Some(Instant::now());
            // A batch over the line is not sent: its first part carries the
            // count, and that is enough to be told it is too much.
            let limit =
                usize::try_from(self.settings.mesh_wire / DATAGRAM as u64).unwrap_or(usize::MAX);
            let count = wazzup.parts.len();
            let sendable = if count > limit { 1 } else { count };
            let mut owing = false;
            let peers: Vec<String> = self
                .peers
                .iter()
                .filter(|peer| wazzup.asks(peer) && self.still_heard(peer))
                .cloned()
                .collect();
            for peer in &peers {
                let unanswered = wazzup.unanswered(peer);
                owing |= !unanswered.is_empty();
                let to = self.address(peer);
                for n in unanswered {
                    if usize::try_from(n).is_ok_and(|n| n < sendable) {
                        let _ = self.send(&self.wazzup_part(wazzup, n), to);
                    }
                }
            }
            if first && wazzup.only.is_none() {
                // Peers not heard yet are told by whoever can hear.
                let mut to = vec![self.to];
                to.extend(self.named_addresses());
                for n in 0..sendable {
                    for to in &to {
                        let _ = self.send(&self.wazzup_part(wazzup, n as u64), (*to, 0));
                    }
                }
            }
            owing || first
        });
        self.asking.append(&mut asking);
    }

    fn wazzup_part(&self, wazzup: &Asking, n: u64) -> Message {
        let lines = wazzup
            .parts
            .get(usize::try_from(n).unwrap_or(usize::MAX))
            .map(|part| part.iter().map(|one| one.line.clone()).collect())
            .unwrap_or_default();
        Message::Wazzup {
            sender: self.settings.node.clone(),
            start: self.start,
            since: self.since,
            id: wazzup.id,
            n,
            count: wazzup.parts.len() as u64,
            lines,
        }
    }

    fn still_heard(&self, peer: &str) -> bool {
        self.heard_at
            .get(peer)
            .is_some_and(|at| at.elapsed() < HEARD_WITHIN)
    }

    /// Answers one part of a peer's wazzup from the rows here, and on its
    /// first part makes what that peer is owed due at once.
    fn answer_wazzup(&mut self, place: &mut dyn Place, wazzup: Wazzup) -> Result<(), Error> {
        let Wazzup {
            peer,
            start,
            id,
            n,
            count,
            lines,
        } = wazzup;
        let too_much = count.saturating_mul(DATAGRAM as u64) > self.settings.mesh_wire
            || self.outbox.owed_bytes(&peer) > self.settings.mesh_wire;
        let rows = if too_much {
            Vec::new()
        } else {
            self.local.rows_now(place, &lines)?
        };
        if !too_much && self.woken_by.insert(peer.clone(), (start, id)) != Some((start, id)) {
            let sending = &self.sending;
            self.outbox
                .all_due_now(&peer, |change| sending.sends(change));
        }
        let answer = Message::Answer {
            sender: self.settings.node.clone(),
            id,
            n,
            too_much,
            rows,
        };
        // A lost answer is asked for again.
        let _ = self.send(&answer, self.address(&peer));
        Ok(())
    }

    /// Settles what one answer proves, and names the rest once the peer has
    /// answered every part.
    fn take_answer(&mut self, place: &mut dyn Place, answer: Answer) -> Result<(), Error> {
        let Answer {
            peer,
            id,
            n,
            too_much,
            rows,
        } = answer;
        let Some(at) = self.asking.iter().position(|wazzup| wazzup.id == id) else {
            return Ok(());
        };
        if too_much {
            self.asking[at].answered_all(&peer);
            self.told_too_much_by(&peer, place.address())?;
            return Ok(());
        }
        let part = self.asking[at]
            .parts
            .get(usize::try_from(n).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or_default();
        // An answer that does not match its part is asked for again.
        if rows.len() != part.len() || !self.asking[at].answered(&peer, n) {
            return Ok(());
        }
        let lines: Vec<Line> = part.iter().map(|one| one.line.clone()).collect();
        let mine = self.local.rows_now(place, &lines)?;
        for ((asked, row), here) in part.iter().zip(&rows).zip(&mine) {
            match settle((asked.from, asked.line.to), here.hash, row) {
                Settled::Has => {
                    if let Some(acked) = self.outbox.ack(&asked.signature, &peer) {
                        self.acked(&peer, acked);
                    }
                }
                Settled::Send if !self.sending.sends(&asked.signature) => {
                    self.outbox.due_now(&asked.signature, &peer);
                }
                Settled::Send => {}
                Settled::Named => self.asking[at].name(&peer, asked),
            }
        }
        if let Some(line) = self.asking[at].take_named(&peer) {
            println!("{line}");
        }
        Ok(())
    }

    /// A peer says there is too much for the wire: the record says how to
    /// catch up, once, and names what this node has not had acknowledged.
    fn told_too_much_by(&mut self, peer: &str, place: &str) -> Result<(), Error> {
        if self.told_too_much {
            return Ok(());
        }
        self.told_too_much = true;
        println!(
            "smugglr mesh: {peer} says there is too much for the wire. Catch up with: smugglr \
             stash <a peer's place> <dir> --key-file <path> --node <name> --whole, then smugglr \
             retrieve {place} <dir> --key-file <path> --node {}",
            self.settings.node
        );
        let named: Vec<String> = self
            .outbox
            .owed_changes(Some(peer))?
            .iter()
            .map(|envelope| {
                format!(
                    "{} {:?} (change {})",
                    envelope.change.table,
                    envelope.change.key,
                    hex(&envelope.signature)
                )
            })
            .collect();
        if !named.is_empty() {
            println!("{}", named_line(peer, &named));
        }
        Ok(())
    }

    /// A person has taken `node` off the mesh: nothing more is owed to it,
    /// and the peers are told, once.
    fn hear_remove(&mut self, node: &str) {
        if !self.removed.insert(node.to_owned()) {
            return;
        }
        self.outbox.remove_target(node);
        self.peers.remove(node);
        self.silent.remove(node);
        self.save_removed();
        println!("smugglr mesh: {node} is taken off the mesh; nothing more is owed to it");
        let remove = Message::Remove {
            sender: self.settings.node.clone(),
            node: node.to_owned(),
        };
        let to: BTreeSet<(SocketAddr, usize)> = self.destinations();
        for to in to {
            let _ = self.send(&remove, to);
        }
    }

    fn save_removed(&self) {
        let text = serde_json::json!(self.removed).to_string();
        // A failed write costs this only after a restart, when the node is
        // told again or a person says it again.
        let _ = self.settings.store.replace(&self.removed_file, &text);
    }

    /// What the peers setting names now.
    fn named_addresses(&self) -> Vec<SocketAddr> {
        self.named
            .lock()
            .map(|named| named.clone())
            .unwrap_or_default()
    }

    /// Keeps a wazzup's part, or an answer to one, for where the database
    /// can be read. The first copies of a start, before it joins, ask only
    /// after the name.
    fn queue(&mut self, message: Message) {
        match message {
            Message::Wazzup {
                sender,
                start,
                id,
                n,
                count,
                lines,
                ..
            } if id != NAME_ONLY => self.wazzups.push(Wazzup {
                peer: sender,
                start,
                id,
                n,
                count,
                lines,
            }),
            Message::Answer {
                sender,
                id,
                n,
                too_much,
                rows,
            } => self.answers.push(Answer {
                peer: sender,
                id,
                n,
                too_much,
                rows,
            }),
            _ => {}
        }
    }

    /// Notes that `sender` was heard at `from`.
    fn note_sender(&mut self, sender: &str, message: &Message, from: SocketAddr) {
        if self.removed.contains(sender) {
            // A node heard again is not gone after all, and its changes must
            // not be lost for a person's mistake or a laptop that slept.
            println!("smugglr mesh: {sender} was taken off the mesh and is heard again");
            self.removed.remove(sender);
            self.save_removed();
        }
        self.heard_at.insert(sender.to_owned(), Instant::now());
        let moved = self.addresses.insert(sender.to_owned(), from) != Some(from);
        if moved && matches!(message, Message::Announce { .. } | Message::Wazzup { .. }) {
            // A peer heard for the first time, or at a new address, hears
            // this node back, so each knows the other.
            let _ = self.send(
                &Message::Announce {
                    sender: self.settings.node.clone(),
                },
                self.address(sender),
            );
        }
        if self.peers.insert(sender.to_owned()) {
            self.outbox.heard(sender);
            self.sending.heard(sender);
        }
    }

    /// Takes in one message from another node; a change it does not have
    /// yet joins `batch`.
    fn hear(&mut self, message: Message, from: SocketAddr, batch: &mut Vec<(String, Envelope)>) {
        if let Message::Remove { node, .. } = &message {
            self.hear_remove(&node.clone());
            return;
        }
        let sender = message.sender().to_owned();
        if sender == self.settings.node {
            self.hear_own_name(&message, from);
            return;
        }
        self.note_sender(&sender, &message, from);
        match message {
            Message::Announce { .. } | Message::NameTaken { .. } | Message::Remove { .. } => {}
            message @ (Message::Wazzup { .. } | Message::Answer { .. }) => self.queue(message),
            Message::Part {
                sender,
                id,
                n,
                count,
                data,
            } => self.hear_part(&sender, id, (n, count), &data, (from, batch)),
            Message::Refused { sender, maker, id } => {
                if maker == self.settings.node {
                    // Decided at the peer: acknowledged, and sent no more.
                    for change in self.sending.refused(&sender, &id) {
                        if let Some(acked) = self.outbox.ack(&change, &sender) {
                            self.acked(&sender, acked);
                        }
                    }
                }
            }
            Message::Parts {
                sender,
                maker,
                id,
                have,
                missing,
            } => {
                if maker == self.settings.node {
                    self.hear_parts(&sender, &id, &have, &missing);
                }
            }
            Message::Ack {
                sender,
                maker,
                changes,
            } => {
                if maker == self.settings.node {
                    for change in &changes {
                        if let Some(acked) = self.outbox.ack(change, &sender) {
                            self.acked(&sender, acked);
                        }
                    }
                }
            }
            Message::Change {
                sender,
                envelope,
                resend,
            } => {
                if envelope.node == self.settings.node {
                    return;
                }
                match self.heard.get_mut(&envelope.signature) {
                    Some(heard) if heard.has => {
                        heard.copies += 1;
                        if sender == envelope.node {
                            let maker = envelope.node.clone();
                            self.acks(BTreeMap::from([(maker, vec![envelope.signature])]));
                        }
                    }
                    Some(_) => batch.push((sender, *envelope)),
                    None => {
                        // Its first copy never arrived: lost on the way.
                        self.lost += usize::from(resend);
                        self.heard.insert(
                            envelope.signature,
                            Heard {
                                has: false,
                                copies: 0,
                                sunset: envelope.sunset,
                            },
                        );
                        batch.push((sender, *envelope));
                    }
                }
            }
        }
    }

    /// Takes in part `n` of `count` of a change from its maker `sender`; the
    /// whole change, once its last part arrives, is heard as if it came in
    /// one datagram.
    fn hear_part(
        &mut self,
        sender: &str,
        id: Id,
        (n, count): (u64, u64),
        data: &[u8],
        (from, batch): (SocketAddr, &mut Vec<(String, Envelope)>),
    ) {
        if let Some(Message::Change {
            envelope, resend, ..
        }) = self
            .receiving
            .part(sender, id, (n, count), data)
            .and_then(|whole| decode(&whole))
        {
            let change = Message::Change {
                sender: sender.to_owned(),
                envelope,
                resend,
            };
            self.hear(change, from, batch);
        }
    }

    /// `peer` has the parts `have` of this node's change keyed by `id`, and
    /// misses `missing`; each first acknowledgement grows the window as a
    /// change's does.
    fn hear_parts(&mut self, peer: &str, id: &Id, have: &[Range], missing: &[Range]) {
        let now = Instant::now();
        for round_trip in self.sending.ack(peer, id, have, missing, now) {
            self.acked(
                peer,
                Acked {
                    first: true,
                    seq: 0,
                    round_trip,
                },
            );
        }
    }

    /// Applies `batch` with the changes still waiting, acknowledging and
    /// arranging an echo for each it applies, and acknowledging a change it
    /// already had only when the copy came from its maker.
    fn apply(
        &mut self,
        place: &mut dyn Place,
        batch: Vec<(String, Envelope)>,
    ) -> Result<(), Error> {
        let epoch = now();
        let from_maker: BTreeSet<[u8; 32]> = batch
            .iter()
            .filter(|(sender, envelope)| *sender == envelope.node)
            .map(|(_, envelope)| envelope.signature)
            .collect();
        let mut seen = BTreeSet::new();
        let envelopes: Vec<Envelope> = std::mem::take(&mut self.waiting)
            .into_iter()
            .chain(batch.into_iter().map(|(_, envelope)| envelope))
            .filter(|envelope| seen.insert(envelope.signature))
            .collect();
        // One decision per signing key; a change no held key signed, or of
        // a table whose shape differs, is refused.
        let mut by_key: Vec<Vec<Envelope>> = vec![Vec::new(); self.sign.len()];
        let (most, address) = limit(place, self.settings);
        let mut acks: BTreeMap<String, Vec<[u8; 32]>> = BTreeMap::new();
        for envelope in envelopes {
            // A value over this node's limit (FR-SMUGGLR-071): named, and
            // decided, so acknowledged and not resent.
            let over = self.sign.iter().any(|key| envelope.verify(key))
                && self
                    .local
                    .table(&envelope.change.table)
                    .is_some_and(|table| refuse(&envelope, table, (most, &address)));
            if over {
                self.heard.insert(
                    envelope.signature,
                    Heard {
                        has: true,
                        copies: 0,
                        sunset: envelope.sunset,
                    },
                );
                acks.entry(envelope.node.clone())
                    .or_default()
                    .push(envelope.signature);
                continue;
            }
            match self.sign.iter().position(|key| envelope.verify(key)) {
                Some(_) if !self.local.same_shape(&envelope) => eprintln!(
                    "smugglr mesh: refused a change to {} from {}: its table's shape differs here",
                    envelope.change.table, envelope.node
                ),
                Some(at) => by_key[at].push(envelope),
                None => eprintln!(
                    "smugglr mesh: refused a change to {} from {}: key {} is not one this node holds",
                    envelope.change.table, envelope.node, envelope.key_id
                ),
            }
        }
        for (at, envelopes) in by_key.into_iter().enumerate() {
            if envelopes.is_empty() {
                continue;
            }
            let sign = self.sign[at];
            let applied = self
                .local
                .apply(place, self.settings, &sign, &envelopes, epoch)?;
            let wrote: BTreeSet<[u8; 32]> = applied.wrote.iter().copied().collect();
            let has: BTreeSet<[u8; 32]> = applied.has.iter().copied().collect();
            let decided: BTreeSet<[u8; 32]> = applied.decided.iter().copied().collect();
            for envelope in envelopes {
                let change = envelope.signature;
                if decided.contains(&change) {
                    // Stopped or dropped and named: decided, so acknowledged,
                    // and its maker stops resending it (FR-SMUGGLR-078 rev
                    // 24).
                    self.heard.insert(
                        change,
                        Heard {
                            has: true,
                            copies: 0,
                            sunset: envelope.sunset,
                        },
                    );
                    acks.entry(envelope.node.clone()).or_default().push(change);
                    continue;
                }
                if !has.contains(&change) {
                    continue;
                }
                self.heard.insert(
                    change,
                    Heard {
                        has: true,
                        copies: 0,
                        sunset: envelope.sunset,
                    },
                );
                if wrote.contains(&change) || from_maker.contains(&change) {
                    acks.entry(envelope.node.clone()).or_default().push(change);
                }
                if wrote.contains(&change) {
                    // Echo is redundancy, not repair: past ECHOES waiting,
                    // the oldest is let go rather than hold a flood's every
                    // change in memory.
                    if self.echoes.len() >= ECHOES {
                        self.echoes.pop_front();
                    }
                    self.echoes
                        .push_back((Instant::now() + echo_wait(), envelope));
                }
            }
            self.waiting.extend(applied.waiting);
        }
        self.acks(acks);
        Ok(())
    }

    /// What `peer`'s acknowledgement of one change tells: the window grows
    /// on a first acknowledgement; the peer's round trip is measured, and a
    /// round trip far beyond it is acknowledgements slowing.
    fn acked(&mut self, peer: &str, acked: Acked) {
        if let Some((since, _)) = self.silent.remove(peer) {
            println!(
                "smugglr mesh: {peer} is acknowledging again after {}",
                span(since.elapsed().as_secs())
            );
        }
        if acked.first {
            self.window.acked();
        }
        let now = Instant::now();
        let first = acked.round_trip.unwrap_or(Duration::from_secs(1));
        let timing = self.timing.entry(peer.to_owned()).or_insert(Timing {
            round_trip: first,
            variation: first / 2,
            last_ack: now,
            highest: 0,
        });
        timing.last_ack = now;
        timing.highest = timing.highest.max(acked.seq);
        if let Some(sample) = acked.round_trip {
            let slowing = sample > timing.round_trip * 4 && sample > SLOWING;
            timing.measure(sample);
            if slowing {
                self.window.congested("acknowledgements slowing");
            }
        }
    }

    /// Refuses, loudly, a change the system will not send as one datagram:
    /// names its row and the catch-up, and lets it go from the outbox so it
    /// never holds up the changes after it.
    fn refuse_too_large(&mut self, envelope: &Envelope) {
        println!(
            "smugglr mesh: a change to {} {:?} is too large for one datagram and is not sent; \
             catch it up with smugglr stash and smugglr retrieve",
            envelope.change.table, envelope.change.key
        );
        self.outbox.forget(&envelope.signature);
    }

    /// Acknowledges each maker's changes, [`ACKS`] to a datagram.
    fn acks(&self, acks: BTreeMap<String, Vec<[u8; 32]>>) {
        for (maker, changes) in acks {
            for changes in changes.chunks(ACKS) {
                // A lost acknowledgement is covered by the maker's resend.
                let _ = self.send(
                    &Message::Ack {
                        sender: self.settings.node.clone(),
                        maker: maker.clone(),
                        changes: changes.to_vec(),
                    },
                    self.address(&maker),
                );
            }
        }
    }

    /// The message of `envelope` sent as `resend`, and whether it is too
    /// large for one datagram, so it goes in parts.
    fn message(&self, envelope: &Envelope, resend: bool) -> (Vec<u8>, bool) {
        let message = encode(&Message::Change {
            sender: self.settings.node.clone(),
            envelope: Box::new(envelope.clone()),
            resend,
        });
        let large = sealed_len(message.len()) > DATAGRAM;
        (message, large)
    }

    /// Sends `pieces`, each unicast to its peer; how many went, and whether
    /// the send buffer was full.
    fn send_pieces(&self, pieces: Vec<Piece>) -> (usize, bool) {
        let mut sent = 0;
        for piece in pieces {
            let part = Message::Part {
                sender: self.settings.node.clone(),
                id: piece.id,
                n: piece.n,
                count: piece.count,
                data: piece.data,
            };
            if self.send(&part, self.address(&piece.peer)).is_err() {
                return (sent, true);
            }
            sent += 1;
        }
        (sent, false)
    }

    /// Sends what is due this tick: first sends within the window, then
    /// resends within the quarter, then echoes. A change too large for one
    /// datagram goes in parts, which share the window and the quarter. A
    /// full send buffer ends the tick.
    fn send_due(&mut self) -> Result<(), Error> {
        let instant = Instant::now();
        if self.counted.elapsed() >= COUNTED {
            // The quarter is of what the node has sent lately.
            self.counted = instant;
            self.sent.set(self.sent.get() / 2);
            self.resent.set(self.resent.get() / 2);
        }
        let outbox = &self.outbox;
        self.sending
            .retain(|change, peer| outbox.owes(change, peer));
        let Some((firsts, mut sent)) = self.send_firsts()? else {
            return Ok(());
        };
        // Parts take turns with first sends under the same window: a change
        // in parts already holds one place in it, waiting on its parts, so
        // its parts are held to the window on their own and never wait on
        // it.
        let room = self
            .window
            .whole()
            .saturating_sub(self.sending.in_flight())
            .min(PER_TICK.saturating_sub(sent));
        let parts = self.sending.firsts(room, instant);
        let first_parts = parts.len();
        let (went, full) = self.send_pieces(parts);
        sent += went;
        if full {
            self.window.congested(FULL);
            return Ok(());
        }
        // When no first send could go (none left, or the window is held by
        // changes waiting on repair), repair is all the node can do, and the
        // quarter would leave it waiting on sends that cannot come.
        let quarter = if firsts > 0 || first_parts > 0 {
            self.resend_room()
        } else if self.outbox.unsent() == 0 {
            self.resend_room().max(IDLE_RESENDS)
        } else {
            // The window is held by changes waiting on repair: just enough
            // resends to free it.
            self.resend_room().max(BLOCKED_RESENDS)
        };
        let room = quarter.min(PER_TICK.saturating_sub(sent));
        let Some((resent, changes)) = self.resend_due(instant, room)? else {
            return Ok(());
        };
        sent += resent;
        self.say_resent(changes, instant);
        self.send_echoes(instant, sent);
        Ok(())
    }

    /// How many of the node's changes hold a place in the window.
    fn in_flight(&self) -> usize {
        let now = Instant::now();
        self.outbox.in_flight(now, |peer| {
            self.heard_at
                .get(peer)
                .is_some_and(|at| now.duration_since(*at) < HEARD_WITHIN)
        })
    }

    /// How many first sends, of changes or parts, the window has room for,
    /// `sent` datagrams already gone this tick.
    fn room(&self, sent: usize) -> usize {
        self.window
            .whole()
            .saturating_sub(self.in_flight() + self.sending.in_flight())
            .min(PER_TICK.saturating_sub(sent))
    }

    /// Sends the first sends the window has room for, each change in one
    /// datagram to each peer, or begun in parts; gives how many changes it
    /// took and how many datagrams went, `None` once the send buffer is
    /// full.
    fn send_firsts(&mut self) -> Result<Option<(usize, usize)>, Error> {
        let to = self.destinations();
        let firsts = self.outbox.first_sends(self.room(0))?;
        let made = Instant::now();
        self.made
            .extend(firsts.iter().map(|envelope| (envelope.signature, made)));
        let mut sent = 0;
        'firsts: for (at, envelope) in firsts.iter().enumerate() {
            let (message, large) = self.message(envelope, false);
            if large {
                self.sending.start(
                    envelope.signature,
                    (envelope.change.from, envelope.change.to.unwrap_or_default()),
                    message,
                    &self.peers,
                );
                continue;
            }
            for to in &to {
                match self.change(envelope, false, *to) {
                    Ok(()) => sent += 1,
                    Err(err) if too_large(&err) => {
                        self.refuse_too_large(envelope);
                        continue 'firsts;
                    }
                    Err(_) => {
                        self.window.congested(FULL);
                        self.outbox.unsend(&firsts[at..]);
                        return Ok(None);
                    }
                }
            }
        }
        Ok(Some((firsts.len(), sent)))
    }

    /// Resends up to `room` datagrams at `instant`: changes due, a change in
    /// parts as the parts its peers miss; gives how many datagrams went and
    /// how many changes, `None` once the send buffer is full.
    fn resend_due(
        &mut self,
        instant: Instant,
        room: usize,
    ) -> Result<Option<(usize, usize)>, Error> {
        let sending = &self.sending;
        let resends = self
            .outbox
            .resends(instant, &self.timing, room, |change, peer| {
                sending.busy(change, peer, instant)
            })?;
        let (mut resent, mut changes) = (0, 0);
        'resends: for (envelope, owed) in &resends {
            let (message, large) = self.message(envelope, true);
            if large {
                if !self.sending.sends(&envelope.signature) {
                    self.sending.start(
                        envelope.signature,
                        (envelope.change.from, envelope.change.to.unwrap_or_default()),
                        message,
                        &self.peers,
                    );
                }
                for peer in owed {
                    self.sending.again(&envelope.signature, peer);
                }
                changes += 1;
                continue;
            }
            // One unicast to each owed peer elsewhere; one broadcast covers
            // those on this machine or not yet located.
            let mut to: BTreeSet<(SocketAddr, usize)> =
                owed.iter().map(|peer| self.address(peer)).collect();
            if to.is_empty() {
                to.insert((self.to, 0));
            }
            for to in to {
                if resent >= room {
                    break 'resends;
                }
                match self.change(envelope, true, to) {
                    Ok(()) => {}
                    Err(err) if too_large(&err) => {
                        self.refuse_too_large(envelope);
                        continue 'resends;
                    }
                    Err(_) => {
                        self.window.congested(FULL);
                        return Ok(None);
                    }
                }
                self.resent.set(self.resent.get() + 1);
                resent += 1;
            }
            changes += 1;
        }
        // The parts a peer misses, within what is left of the quarter.
        let timing = &self.timing;
        let pieces = self.sending.resends(
            instant,
            |peer| {
                timing
                    .get(peer)
                    .filter(|timing| timing.acknowledging(instant))
                    .map(Timing::wait)
            },
            room.saturating_sub(resent),
        );
        let (went, full) = self.send_pieces(pieces);
        self.resent.set(self.resent.get() + went as u64);
        resent += went;
        if full {
            self.window.congested(FULL);
            return Ok(None);
        }
        Ok(Some((resent, changes)))
    }

    /// Names each peer that owes acknowledgements and has given none for
    /// [`SILENCE`], at most every [`SAY_SILENCE`] while that lasts.
    fn say_silence(&mut self, now: Instant) -> Result<(), Error> {
        let peers: Vec<String> = self.peers.iter().cloned().collect();
        for peer in peers {
            let Some((count, oldest)) = self.outbox.owed(&peer) else {
                self.silent.remove(&peer);
                continue;
            };
            let since = self
                .timing
                .get(&peer)
                .map_or(oldest, |timing| timing.last_ack.max(oldest));
            let said = self.silent.get(&peer).map(|(_, said)| *said);
            if silence_due(
                now.duration_since(since),
                said.map(|at| now.duration_since(at)),
            ) {
                let heard = self.heard_at.get(&peer).map(|at| now.duration_since(*at));
                println!(
                    "{}",
                    silence_line(&peer, now.duration_since(since), count, heard)
                );
                self.silent.insert(peer.clone(), (since, now));
                // A peer still heard is holding something: ask it, so what
                // its rows prove is settled with nobody watching.
                let asked = self
                    .asking
                    .iter()
                    .any(|wazzup| wazzup.only.as_deref() == Some(&peer));
                if self.still_heard(&peer) && !asked {
                    self.ask(Some(peer))?;
                }
            }
        }
        Ok(())
    }

    /// Notes `changes` resent, and says so at most once a minute.
    fn say_resent(&mut self, changes: usize, instant: Instant) {
        self.resent_unsaid += changes;
        if self.resent_unsaid > 0 && self.said_resent.is_none_or(|at| at.elapsed() >= SAY_EVERY) {
            println!(
                "smugglr mesh: resent {} {} a peer had not acknowledged",
                self.resent_unsaid,
                if self.resent_unsaid == 1 {
                    "change"
                } else {
                    "changes"
                }
            );
            self.resent_unsaid = 0;
            self.said_resent = Some(instant);
        }
    }

    /// Sends the echoes due at `instant`, paced, `sent` datagrams already
    /// gone this tick.
    fn send_echoes(&mut self, instant: Instant, mut sent: usize) {
        // Echoes are paced too: each loops back to this node's own socket
        // as well, and a flood's echoes sent at once would fill it.
        let mut echoed = 0;
        while sent < PER_TICK
            && echoed < ECHOES_PER_TICK
            && self.echoes.front().is_some_and(|(due, _)| instant >= *due)
        {
            let Some((_, envelope)) = self.echoes.pop_front() else {
                break;
            };
            let copies = self
                .heard
                .get(&envelope.signature)
                .map_or(0, |heard| heard.copies);
            // Echo is redundancy: a change in parts is not echoed.
            if copies < self.settings.k && !self.message(&envelope, false).1 {
                match self.change(&envelope, false, (self.to, 0)) {
                    Ok(()) => {}
                    // Its maker refuses it too; an echo just lets it go.
                    Err(err) if too_large(&err) => continue,
                    Err(_) => {
                        self.window.congested(FULL);
                        return;
                    }
                }
                sent += 1;
                echoed += 1;
            }
        }
    }
}

impl std::fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("node", &self.settings.node)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

/// Whether the system refused a datagram for its size (EMSGSIZE): rows up
/// to that size go by IP fragmentation; past it, a change cannot go as one.
fn too_large(err: &std::io::Error) -> bool {
    let code = err.raw_os_error();
    if cfg!(any(target_os = "linux", target_os = "android")) {
        code == Some(90)
    } else if cfg!(windows) {
        code == Some(10_040)
    } else {
        code == Some(40)
    }
}

/// How long the node was away, when its last run, kept at `ran` in
/// smugglr's own files, ended longer ago than the sunset (the graveyard
/// setting); `None` on a first run or a short absence.
fn away(settings: &Settings, ran: &str) -> Result<Option<u64>, Error> {
    let last = settings
        .store
        .read(ran)?
        .and_then(|text| text.trim().parse::<u64>().ok());
    Ok(last
        .map(|last| now().saturating_sub(last))
        .filter(|away| *away > settings.graveyard))
}

/// What a node back from past the sunset prints: how long it was away, and
/// how to catch up (FR-SMUGGLR-078 rev 22).
fn catch_up(away: u64, settings: &Settings, place: &str) -> String {
    format!(
        "smugglr mesh: this node has been away {}, longer than the sunset ({}); \
         changes made meanwhile may not reach it. Catch up with: smugglr stash \
         <a peer's place> <dir> --key-file <path> --node <name> --whole, then smugglr \
         retrieve {place} <dir> --key-file <path> --node {}",
        span(away),
        span(settings.graveyard),
        settings.node
    )
}

/// `seconds` as a person reads a span: days, hours, minutes or seconds.
fn span(seconds: u64) -> String {
    let (count, unit) = match seconds {
        s if s >= 86_400 => (s / 86_400, "day"),
        s if s >= 3_600 => (s / 3_600, "hour"),
        s if s >= 60 => (s / 60, "minute"),
        s => (s, "second"),
    };
    format!("{count} {unit}{}", if count == 1 { "" } else { "s" })
}

/// The address this machine's broadcasts leave from: the system picks it
/// for a socket connected toward the broadcast address, and sends nothing.
fn own_address(port: u16) -> Option<IpAddr> {
    let probe = UdpSocket::bind("0.0.0.0:0").ok()?;
    probe.set_broadcast(true).ok()?;
    probe.connect(("255.255.255.255", port)).ok()?;
    Some(probe.local_addr().ok()?.ip())
}

/// The unicast address for a peer last seen at `seen`; `None` for a peer
/// not yet seen, or in the node's own socket group (on this machine, one of
/// `own` or loopback, at the node's `port`), which is sent to by broadcast.
fn unicast(seen: Option<&SocketAddr>, own: &BTreeSet<IpAddr>, port: u16) -> Option<SocketAddr> {
    let seen = *seen?;
    let ip = seen.ip();
    let shares = seen.port() == port && (ip.is_loopback() || own.contains(&ip));
    (!shares).then_some(seen)
}

/// Now, in seconds since the Unix epoch.
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs())
}

/// A random wait of 2 to 4 seconds before an echo.
fn echo_wait() -> Duration {
    Duration::from_millis(2_000 + u64::from(OsRng.next_u32() % 2_001))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    use super::{SAY_SILENCE, SILENCE, silence_due, silence_line, span, unicast};

    #[test]
    fn a_peer_is_named_after_the_silence_and_then_only_at_a_slow_rate() {
        let second = Duration::from_secs(1);
        assert!(!silence_due(SILENCE.saturating_sub(second), None));
        assert!(silence_due(SILENCE, None));
        assert!(!silence_due(
            SILENCE * 2,
            Some(SAY_SILENCE.saturating_sub(second))
        ));
        assert!(silence_due(SILENCE * 2, Some(SAY_SILENCE)));
    }

    #[test]
    fn the_silence_line_names_the_peer_how_long_and_how_many() {
        let hour = Duration::from_secs(3_600);
        assert_eq!(
            silence_line("rzr", hour * 32, 32, Some(Duration::from_secs(4))),
            "smugglr mesh: rzr has acknowledged nothing for 1 day; 32 changes owed to it, last heard 4 seconds ago"
        );
        assert_eq!(
            silence_line("rzr", hour, 1, None),
            "smugglr mesh: rzr has acknowledged nothing for 1 hour; 1 change owed to it, not heard since this node started"
        );
    }

    #[test]
    fn a_span_reads_in_its_largest_whole_unit() {
        assert_eq!(span(3 * 86_400 + 5), "3 days");
        assert_eq!(span(12 * 3_600), "12 hours");
        assert_eq!(span(60), "1 minute");
        assert_eq!(span(4), "4 seconds");
    }

    const LAPTOP: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 7));

    #[test]
    fn a_peer_elsewhere_is_sent_to_at_the_address_its_datagrams_came_from() {
        let desktop = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 20)), 31337);
        assert_eq!(
            unicast(Some(&desktop), &BTreeSet::from([LAPTOP]), 31337),
            Some(desktop)
        );
    }

    #[test]
    fn a_peer_on_this_machine_at_another_port_is_sent_to_unicast() {
        let beside = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40000);
        assert_eq!(
            unicast(Some(&beside), &BTreeSet::from([LAPTOP]), 31337),
            Some(beside)
        );
    }

    #[test]
    fn a_peer_sharing_the_port_on_this_machine_or_not_yet_seen_is_sent_to_by_broadcast() {
        let own = BTreeSet::from([LAPTOP]);
        assert_eq!(
            unicast(Some(&SocketAddr::new(LAPTOP, 31337)), &own, 31337),
            None
        );
        let loopback = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 31337);
        assert_eq!(unicast(Some(&loopback), &own, 31337), None);
        assert_eq!(unicast(None, &own, 31337), None);
    }
}
