//! One node: finds its own changes and sends them, applies, acknowledges
//! and echoes the changes it hears, and resends its own until every peer
//! has them or their sunset passes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chacha20poly1305::aead::OsRng;
use chacha20poly1305::aead::rand_core::RngCore;
use serde_json::{Value, json};
use smugglr_core::envelope::Envelope;
use smugglr_core::hex::hex;
use smugglr_core::record::Status;
use smugglr_core::row_hash::row_hash;
use smugglr_multi::{Blob, retrieve, stash};
use smugglr_place::Place;
use socket2::{Domain, Protocol, Socket, Type};

use crate::wire::{Message, open, seal};
use crate::{Error, Settings};

/// How long a node waits for a datagram before it looks at its clocks.
const TICK: Duration = Duration::from_millis(100);

/// How often a node looks for its own changes.
const SCAN: Duration = Duration::from_millis(500);

/// The first resend's wait; each later one doubles, up to [`LAST_RESEND`].
const FIRST_RESEND: Duration = Duration::from_secs(10);

/// The longest wait between two resends.
const LAST_RESEND: Duration = Duration::from_secs(160);

/// The file in a node's folder: its name and the peers it has heard.
const FILE: &str = "node.json";

/// Runs the node whose database is `place` until `stop` is set.
///
/// # Errors
/// When the node holds no key, its socket cannot be opened, or its place or
/// what smugglr keeps for it cannot be read or written.
pub fn run(place: &mut dyn Place, settings: &Settings, stop: &AtomicBool) -> Result<(), Error> {
    let key = *settings.keys.last().ok_or(Error::NoKey)?;
    let socket = bind(settings.port)?;
    let mut node = Node::open(place, settings, socket, key)?;
    node.send(&Message::Hello {
        sender: node.me.clone(),
    })?;
    let mut scanned: Option<Instant> = None;
    let mut buffer = vec![0; 65_536];
    while !stop.load(Ordering::Relaxed) {
        // A busy database or a datagram that could not go out is a moment's
        // trouble: the node says so and goes on, and resends cover the gap.
        if let Err(err) = node.turn(&mut buffer, &mut scanned) {
            eprintln!("smugglr mesh: {err}");
        }
    }
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
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], port)).into())?;
    let socket: UdpSocket = socket.into();
    socket.set_read_timeout(Some(TICK))?;
    Ok(socket)
}

/// A change this node has heard.
#[derive(Debug)]
struct Heard {
    envelope: Envelope,
    /// The node has the change: applied, or already at it.
    has: bool,
    /// Copies heard since the node applied it.
    copies: u32,
    /// When the node echoes it, if it applied it and has not yet.
    echo_at: Option<Instant>,
}

/// One of this node's own changes, and the peers that have not
/// acknowledged it.
#[derive(Debug)]
struct Outgoing {
    envelope: Envelope,
    targets: BTreeSet<String>,
    resend_at: Instant,
    interval: Duration,
}

struct Node<'a> {
    place: &'a mut dyn Place,
    settings: &'a Settings,
    socket: UdpSocket,
    key: [u8; 32],
    me: String,
    peers: BTreeSet<String>,
    file: String,
    heard: HashMap<[u8; 32], Heard>,
    outbox: HashMap<[u8; 32], Outgoing>,
}

impl<'a> Node<'a> {
    /// The node for `place`: its name and peers from smugglr's files, a new
    /// `UUIDv7` on its first run.
    fn open(
        place: &'a mut dyn Place,
        settings: &'a Settings,
        socket: UdpSocket,
        key: [u8; 32],
    ) -> Result<Self, Error> {
        let store = &*settings.store;
        let file = format!("mesh/{}/{FILE}", hex(&row_hash(&["mesh", place.address()])));
        let kept = match store.read(&file)? {
            Some(text) => Some(serde_json::from_str::<Value>(&text).map_err(|_| {
                smugglr_memory::Error::Unreadable {
                    path: store.locate(&file),
                    line: 1,
                }
            })?),
            None => None,
        };
        let me = kept
            .as_ref()
            .and_then(|kept| kept.get("node")?.as_str().map(str::to_owned));
        let peers = kept
            .as_ref()
            .and_then(|kept| kept.get("peers")?.as_array().cloned())
            .unwrap_or_default()
            .iter()
            .filter_map(|peer| peer.as_str().map(str::to_owned))
            .collect();
        let node = Self {
            place,
            settings,
            socket,
            key,
            me: me.clone().unwrap_or_else(uuid_v7),
            peers,
            file,
            heard: HashMap::new(),
            outbox: HashMap::new(),
        };
        if me.is_none() {
            node.save()?;
        }
        Ok(node)
    }

    /// Takes in what arrives within one tick, looks for its own changes
    /// when a look is due, and runs its clocks.
    fn turn(&mut self, buffer: &mut [u8], scanned: &mut Option<Instant>) -> Result<(), Error> {
        match self.socket.recv_from(buffer) {
            Ok((len, _)) => {
                if let Some(message) = open(&buffer[..len], &self.settings.keys) {
                    self.hear(message)?;
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(err) => return Err(err.into()),
        }
        if scanned.is_none_or(|at| at.elapsed() >= SCAN) {
            *scanned = Some(Instant::now());
            self.scan()?;
        }
        self.timers()
    }

    fn save(&self) -> Result<(), Error> {
        let text = json!({"node": self.me, "peers": self.peers}).to_string();
        Ok(self.settings.store.replace(&self.file, &text)?)
    }

    fn send(&self, message: &Message) -> Result<(), Error> {
        self.socket
            .send_to(&seal(message, &self.key), self.settings.send_to)?;
        Ok(())
    }

    /// multi's settings for a stash or retrieve now, with `key`.
    fn multi(&self, key: [u8; 32]) -> smugglr_multi::Settings {
        smugglr_multi::Settings {
            store: self.settings.store.clone(),
            key,
            node: self.me.clone(),
            targets: Vec::new(),
            graveyard: self.settings.graveyard,
            now: now(),
            sync_mode: self.settings.sync_mode,
            steering: self.settings.steering.clone(),
            ignore: self.settings.ignore.clone(),
        }
    }

    /// Sends each row changed here since the last look as one change, and
    /// keeps it to resend. A node's first look only notes every row's hash.
    fn scan(&mut self) -> Result<(), Error> {
        let settings = self.multi(self.key);
        let blob = stash(self.place, &settings, false, |_| Ok(()))?;
        if blob.whole {
            return Ok(());
        }
        for envelope in blob.changes {
            self.send(&Message::Change {
                sender: self.me.clone(),
                envelope: Box::new(envelope.clone()),
            })?;
            if self.peers.is_empty() {
                continue;
            }
            self.outbox.insert(
                envelope.signature,
                Outgoing {
                    envelope,
                    targets: self.peers.clone(),
                    resend_at: Instant::now() + FIRST_RESEND,
                    interval: FIRST_RESEND,
                },
            );
        }
        Ok(())
    }

    /// Takes in one message from another node.
    fn hear(&mut self, message: Message) -> Result<(), Error> {
        let sender = message.sender().to_owned();
        if sender == self.me {
            return Ok(());
        }
        if self.peers.insert(sender.clone()) {
            self.save()?;
            // A new peer hears this node too, so each counts the other.
            self.send(&Message::Hello {
                sender: self.me.clone(),
            })?;
        }
        match message {
            Message::Hello { .. } => Ok(()),
            Message::Ack {
                sender,
                maker,
                change,
            } => {
                if maker == self.me
                    && let Some(outgoing) = self.outbox.get_mut(&change)
                {
                    outgoing.targets.remove(&sender);
                    if outgoing.targets.is_empty() {
                        self.outbox.remove(&change);
                    }
                }
                Ok(())
            }
            Message::Change { sender, envelope } => self.change(&sender, *envelope),
        }
    }

    /// Takes in a copy of a change: applies it the first time it can,
    /// acknowledging and arranging its echo; acknowledges a later copy only
    /// when it comes from the maker.
    fn change(&mut self, sender: &str, envelope: Envelope) -> Result<(), Error> {
        let maker = envelope.node.clone();
        if maker == self.me {
            return Ok(());
        }
        let signature = envelope.signature;
        if let Some(heard) = self.heard.get_mut(&signature)
            && heard.has
        {
            heard.copies += 1;
            if sender == maker {
                self.ack(maker, signature)?;
            }
            return Ok(());
        }
        let (has, applied) = self.receive(&envelope)?;
        let echo_at = applied.then(|| Instant::now() + echo_wait());
        self.heard.insert(
            signature,
            Heard {
                envelope,
                has,
                copies: 0,
                echo_at,
            },
        );
        if applied || (has && sender == maker) {
            self.ack(maker, signature)?;
        }
        Ok(())
    }

    fn ack(&self, maker: String, change: [u8; 32]) -> Result<(), Error> {
        self.send(&Message::Ack {
            sender: self.me.clone(),
            maker,
            change,
        })
    }

    /// Retrieves `envelope` by the envelope rules, verified with the key
    /// that signed it (the newest when none of the node's keys did, so it is
    /// refused). Returns whether the node has the change, and whether this
    /// retrieve applied it.
    fn receive(&mut self, envelope: &Envelope) -> Result<(bool, bool), Error> {
        let key = *self
            .settings
            .keys
            .iter()
            .rev()
            .find(|key| envelope.verify(key))
            .unwrap_or(&self.key);
        let mut blob = Blob {
            node: envelope.node.clone(),
            targets: Vec::new(),
            whole: false,
            stamps: BTreeMap::from([(envelope.change.table.clone(), envelope.stamp)]),
            changes: vec![envelope.clone()],
            graveyard: Vec::new(),
            signature: [0; 32],
            sunset: envelope.sunset,
        };
        blob.sign(&key);
        let settings = self.multi(key);
        let record = retrieve(self.place, &[blob], &settings, &[])?;
        let has = record.status == Status::Done && record.dropped.is_empty();
        Ok((has, has && record.wrote()))
    }

    /// Echoes what is due, resends what is due, and forgets what is past its
    /// sunset.
    fn timers(&mut self) -> Result<(), Error> {
        let (instant, epoch) = (Instant::now(), now());
        let mut echoes = Vec::new();
        for heard in self.heard.values_mut() {
            if heard.echo_at.is_some_and(|at| instant >= at) {
                heard.echo_at = None;
                if heard.copies < self.settings.k {
                    echoes.push(heard.envelope.clone());
                }
            }
        }
        for envelope in echoes {
            self.send(&Message::Change {
                sender: self.me.clone(),
                envelope: Box::new(envelope),
            })?;
        }
        self.heard
            .retain(|_, heard| heard.echo_at.is_some() || epoch <= heard.envelope.sunset);
        self.outbox
            .retain(|_, outgoing| epoch <= outgoing.envelope.sunset);
        let mut resends = Vec::new();
        for outgoing in self.outbox.values_mut() {
            if instant >= outgoing.resend_at {
                outgoing.interval = (outgoing.interval * 2).min(LAST_RESEND);
                outgoing.resend_at = instant + outgoing.interval;
                resends.push(outgoing.envelope.clone());
            }
        }
        for envelope in resends {
            self.send(&Message::Change {
                sender: self.me.clone(),
                envelope: Box::new(envelope),
            })?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for Node<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("me", &self.me)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
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

/// A new `UUIDv7`: the time in milliseconds, then random bits.
fn uuid_v7() -> String {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let mut bytes = [0_u8; 16];
    OsRng.fill_bytes(&mut bytes[6..]);
    bytes[..6].copy_from_slice(&millis.to_be_bytes()[10..]);
    bytes[6] = (bytes[6] & 0x0f) | 0x70;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    )
}

#[cfg(test)]
mod tests {
    use super::uuid_v7;

    #[test]
    fn a_node_name_is_a_uuid_v7() {
        let name = uuid_v7();
        assert_eq!(name.len(), 36);
        assert_eq!(&name[14..15], "7");
        assert!(matches!(&name[19..20], "8" | "9" | "a" | "b"));
    }
}
