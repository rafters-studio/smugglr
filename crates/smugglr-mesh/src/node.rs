//! One node: finds its own changes and sends them, paced; applies the
//! changes it hears in batches, acknowledges and echoes them; and resends
//! its own until every peer has them or their sunset passes.
//!
//! First sends and echoes go out as broadcasts; a maker's resends and every
//! acknowledgement go unicast to the peer's last-seen address at the mesh
//! port (toy Z), because a link retries a unicast frame and does not retry
//! a broadcast one. A peer on this same machine shares the port, where a
//! unicast reaches only one of the sockets on it, so it is sent to by
//! broadcast.
//!
//! A reader thread drains the socket into memory as datagrams arrive, so a
//! long look at the database or a large batch never leaves the socket's
//! buffer to overflow. Everything a node sends is paced: first sends are
//! held to a window that starts small, grows with each change a peer
//! acknowledges and halves on loss or a full send buffer (RFC 8085), and
//! each tick sends at most [`PER_TICK`] datagrams, resends and echoes
//! first.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::OsRng;
use chacha20poly1305::aead::rand_core::RngCore;
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::hex;
use smugglr_core::row_hash::row_hash;
use smugglr_place::Place;
use socket2::{Domain, Protocol, Socket, Type};

use crate::local::Local;
use crate::outbox::Outbox;
use crate::wire::{ACKS, Message, open, seal};
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

/// The largest window: a batch's worth of changes.
const LARGEST: f64 = 20_000.0;

/// How often at most the record names datagrams that did not open.
const SAY_UNOPENED: Duration = Duration::from_secs(60);

/// How often the outbox and what the node has heard are tidied.
const TIDY: Duration = Duration::from_secs(1);

/// Bytes kept free under the backpressure line for the outbox's index.
const INDEX_ROOM: u64 = 1_024;

/// The receive buffer a node asks its system for.
const RECEIVE_BUFFER: usize = 8 << 20;

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
    let folder = format!("mesh/{}", hex(&row_hash(&["mesh", place.address()])));
    let local = Local::open(place, settings, &folder, now())?;
    let outbox = Outbox::open(settings.store.clone(), &format!("{folder}/outbox"), now())?;
    let mut node = Node {
        settings,
        socket,
        to: SocketAddr::from(([255, 255, 255, 255], settings.port)),
        seal: held.iter().rev().map(sealing).collect(),
        sign: held.iter().rev().map(signing).collect(),
        local,
        outbox,
        peers: BTreeSet::new(),
        addresses: HashMap::new(),
        own: BTreeSet::new(),
        heard: HashMap::new(),
        waiting: Vec::new(),
        echoes: VecDeque::new(),
        window: Window::new(),
        scanned: None,
        scan_took: Duration::ZERO,
        scan_full: false,
        lost: 0,
        unopened: 0,
        said_unopened: None,
        tidied: Instant::now(),
    };
    let (tx, rx) = mpsc::channel();
    let reading = AtomicBool::new(true);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut buffer = vec![0; 65_536];
            while reading.load(Ordering::Relaxed) {
                if let Ok((len, from)) = reader.recv_from(&mut buffer)
                    && tx.send((buffer[..len].to_vec(), from.ip())).is_err()
                {
                    break;
                }
            }
        });
        while !stop.load(Ordering::Relaxed) {
            // A busy database or a datagram that could not go out is a
            // moment's trouble: the node says so and goes on, and resends
            // cover the gap.
            if let Err(err) = node.turn(place, &rx) {
                eprintln!("smugglr mesh: {err}");
            }
        }
        reading.store(false, Ordering::Relaxed);
    });
    Ok(())
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

    /// A loss or a full send buffer: the window halves, at most once a
    /// second.
    fn lost(&mut self) {
        if self
            .cut
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(1))
        {
            self.threshold = (self.size / 2.0).max(4.0);
            self.size = self.threshold;
            self.cut = Some(Instant::now());
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
    /// The nodes it has heard.
    peers: BTreeSet<String>,
    /// Each peer's address, as its last datagram came from.
    addresses: HashMap<String, IpAddr>,
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
        rx: &Receiver<(Vec<u8>, IpAddr)>,
    ) -> Result<(), Error> {
        let mut batch: Vec<(String, Envelope)> = Vec::new();
        let mut first = match rx.recv_timeout(TICK) {
            Ok(datagram) => Some(datagram),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        };
        while let Some((datagram, from)) = first.take().or_else(|| rx.try_recv().ok()) {
            match open(&datagram, &self.seal) {
                Some(message) => self.hear(message, from, &mut batch),
                None => self.unopened += 1,
            }
            if batch.len() >= CHUNK {
                break;
            }
        }
        if !batch.is_empty() {
            self.apply(place, batch)?;
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
        // A look that stopped at the backpressure line looks again once
        // acknowledgements have emptied half the outbox, not for every
        // change they settle; any other look waits its turn.
        let due = if self.scan_full {
            self.outbox.bytes() <= self.settings.backpressure / 2
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
            let (found, full) = self.local.changes(
                place,
                self.settings,
                &self.sign[0],
                now(),
                budget,
                |envelope| Outbox::weigh(envelope, peers),
            )?;
            self.scan_full = full;
            if !found.is_empty() {
                self.outbox.add(&found, &self.peers)?;
            }
            self.scanned = Some(Instant::now());
            self.scan_took = started.elapsed();
        }
        self.send_due()?;
        if self.unopened > 0
            && self
                .said_unopened
                .is_none_or(|at| at.elapsed() >= SAY_UNOPENED)
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
        if self.tidied.elapsed() >= TIDY {
            self.tidied = Instant::now();
            let epoch = now();
            self.outbox.prune(epoch)?;
            self.heard.retain(|_, heard| epoch <= heard.sunset);
            if !self.waiting.is_empty() {
                // A change waiting past its sunset is dropped.
                self.apply(place, Vec::new())?;
            }
        }
        Ok(())
    }

    fn send(&self, message: &Message, to: SocketAddr) -> std::io::Result<()> {
        self.socket
            .send_to(&seal(message, &self.seal[0]), to)
            .map(drop)
    }

    fn change(&self, envelope: &Envelope, resend: bool, to: SocketAddr) -> std::io::Result<()> {
        self.send(
            &Message::Change {
                sender: self.settings.node.clone(),
                envelope: Box::new(envelope.clone()),
                resend,
            },
            to,
        )
    }

    /// Where a message for `peer` alone goes: its last-seen address at the
    /// mesh port, or the broadcast address for a peer on this machine or one
    /// whose address is not known.
    fn address(&self, peer: &str) -> SocketAddr {
        unicast(self.addresses.get(peer), &self.own, self.settings.port).unwrap_or(self.to)
    }

    /// Takes in one message from another node; a change it does not have
    /// yet joins `batch`.
    fn hear(&mut self, message: Message, from: IpAddr, batch: &mut Vec<(String, Envelope)>) {
        let sender = message.sender().to_owned();
        if sender == self.settings.node {
            // The node hears its own broadcasts: from its own addresses.
            self.own.insert(from);
            return;
        }
        self.addresses.insert(sender.clone(), from);
        if self.peers.insert(sender.clone()) {
            self.outbox.heard(&sender);
        }
        match message {
            Message::Ack {
                sender,
                maker,
                changes,
            } => {
                if maker == self.settings.node {
                    for change in &changes {
                        if self.outbox.ack(change, &sender) {
                            self.window.acked();
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
        for envelope in envelopes {
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
        let mut acks: BTreeMap<String, Vec<[u8; 32]>> = BTreeMap::new();
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
            for envelope in envelopes {
                let change = envelope.signature;
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
                    self.echoes
                        .push_back((Instant::now() + echo_wait(), envelope));
                }
            }
            self.waiting.extend(applied.waiting);
        }
        self.acks(acks);
        Ok(())
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

    /// Sends what is due this tick: resends, then echoes, then first sends
    /// within the window. A full send buffer ends the tick.
    fn send_due(&mut self) -> Result<(), Error> {
        let instant = Instant::now();
        let (resends, lost) = self.outbox.resends(instant, PER_TICK)?;
        if lost {
            self.window.lost();
        }
        let (mut sent, mut changes) = (0, 0);
        for (envelope, owed) in &resends {
            // One unicast to each owed peer elsewhere; one broadcast covers
            // those on this machine or not yet located.
            let mut to: BTreeSet<SocketAddr> = owed.iter().map(|peer| self.address(peer)).collect();
            if to.is_empty() {
                to.insert(self.to);
            }
            for to in to {
                if self.change(envelope, true, to).is_err() {
                    self.window.lost();
                    return Ok(());
                }
                sent += 1;
            }
            changes += 1;
        }
        if changes > 0 {
            println!(
                "smugglr mesh: resent {changes} {} a peer has not acknowledged",
                if changes == 1 { "change" } else { "changes" }
            );
        }
        while sent < PER_TICK && self.echoes.front().is_some_and(|(due, _)| instant >= *due) {
            let Some((_, envelope)) = self.echoes.pop_front() else {
                break;
            };
            let copies = self
                .heard
                .get(&envelope.signature)
                .map_or(0, |heard| heard.copies);
            if copies < self.settings.k {
                if self.change(&envelope, false, self.to).is_err() {
                    self.window.lost();
                    return Ok(());
                }
                sent += 1;
            }
        }
        let room = self
            .window
            .whole()
            .saturating_sub(self.outbox.in_flight())
            .min(PER_TICK - sent);
        let firsts = self.outbox.first_sends(room)?;
        for (at, envelope) in firsts.iter().enumerate() {
            if self.change(envelope, false, self.to).is_err() {
                self.window.lost();
                self.outbox.unsend(&firsts[at..]);
                break;
            }
        }
        Ok(())
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

/// The unicast address for a peer last seen at `seen`, at the mesh `port`;
/// `None` for a peer not yet seen or on this machine (one of `own`, or
/// loopback), which is sent to by broadcast.
fn unicast(seen: Option<&IpAddr>, own: &BTreeSet<IpAddr>, port: u16) -> Option<SocketAddr> {
    let ip = *seen?;
    (!ip.is_loopback() && !own.contains(&ip)).then(|| SocketAddr::new(ip, port))
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

    use super::unicast;

    #[test]
    fn a_peer_elsewhere_is_sent_to_at_its_address_and_the_mesh_port() {
        let rzr = IpAddr::V4(Ipv4Addr::new(192, 168, 88, 20));
        let own = BTreeSet::from([IpAddr::V4(Ipv4Addr::new(192, 168, 88, 7))]);
        assert_eq!(
            unicast(Some(&rzr), &own, 31337),
            Some(SocketAddr::new(rzr, 31337))
        );
    }

    #[test]
    fn a_peer_on_this_machine_or_not_yet_seen_is_sent_to_by_broadcast() {
        let puck = IpAddr::V4(Ipv4Addr::new(192, 168, 88, 7));
        let own = BTreeSet::from([puck]);
        assert_eq!(unicast(Some(&puck), &own, 31337), None);
        assert_eq!(
            unicast(Some(&IpAddr::V4(Ipv4Addr::LOCALHOST)), &own, 31337),
            None
        );
        assert_eq!(unicast(None, &own, 31337), None);
    }
}
