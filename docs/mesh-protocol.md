# The smugglr mesh protocol

Status: 0.6.1. Written from FR-SMUGGLR-078 (rev 26) and FR-SMUGGLR-079 and checked against the binary. Where a requirement is not built yet, the section says so and names the issue. Current choice of the design team (Sean with smugglr): smugglr carries its own mesh protocol rather than a gossip or membership library, and documents it here for its own code and for programs that use the crates. Revisable.

`smugglr mesh` keeps the copies of one SQLite database in step across machines where nobody is in charge: no server, no hub, no coordinator. Any node can leave and the rest go on. This document says what goes over the wire and what each node does with it. The rules that decide what a change means (apply, skip, hold, conflict, graveyard) belong to `smugglr-core` and are the same for every sync type; the mesh only carries them.

Scope: designed and tested for up to 50 nodes. Acknowledgements grow with the node count and are not suppressed. Hundreds of nodes is outside 0.6.

## Terms

- **Node**: one running `smugglr mesh` process with its database.
- **Change**: one row's move from one hash to another: `{op, table, key, from, to, values}`. `from` is the row's hash before the change, `to` after; the empty hash means the row is absent.
- **Envelope**: a Change plus what the mesh needs to carry it (below).
- **Peer**: a node this node has heard, under the shared key, at the address its datagrams came from.
- **Target**: a peer that has not yet acknowledged a change.
- **Outbox**: the node's list of what it is owed, kept in smugglr's own files.
- **Sunset**: the time after which a change is dead. Set by its sender: the sender's clock plus the graveyard setting (12 hours unless set).
- **Graveyard**: every delete envelope a node has applied, kept until its sunset.
- **Shared key**: the one key every node holds, given with `--key-file`.

## Settings

| Setting | Where | Default | What it does |
|---|---|---|---|
| `--key-file <path>` | flag | none, required | the shared key file (below) |
| `--replay <change hash>` | flag | none | decide a dropped change again and exit (below) |
| `--sync-mode stop\|last` | flag | `stop` | what happens to a row changed here and in a change; `source` and `target` are for two places and are refused |
| `port` | config | 31337 | the UDP port a node broadcasts and listens on |
| `peers` | config | none | addresses to announce to on a network with no broadcast: `host` or `host:port`, the mesh port when none is given |
| `backpressure` | config | 6 MB | the most the outbox holds, in bytes or text such as `"6 MB"` |
| `graveyard` | config | 12 hours | seconds a change is carried; sets each sunset |
| `mesh_wire` | config | 8 MB | the line past which a waking node catches up by stash then retrieve. Not read in 0.6.1 (#521) |

`smugglr mesh ./node.db --key-file ./mesh.key` and `smugglr sync ./node.db --sync-type mesh --key-file ./mesh.key` are the same command and join the same mesh.

## Keys

The key file holds one key per line, base64 of 32 bytes, newest last. A line starting with `#` is a note. A file with one key is the single key.

A node signs and seals with the newest key and accepts the one before it. In 0.6.1 the previous key is accepted with no retirement: the rule that drops it once its last change passes its sunset is not built.

`smugglr mesh keygen --key-file <path>` appends a new key from the operating system's random source, and makes the file when there is none. A key is rotated by appending a new one.

On unix a node refuses to start when its key file is readable by group or others, and says the `chmod 600` that fixes it. Windows is not checked (open: whether Windows ACLs should be). No key appears in output, logs or errors; a key is named only by its `key_id`.

The shared key is never used directly. Two keys are derived from it: one seals datagrams and one signs envelopes, so one key never serves two jobs.

## The envelope

| Field | Meaning |
|---|---|
| `v` | Envelope format version |
| `op` | `up`, `down` or `upsert` |
| `node` | The node that made the change (its maker) |
| `table`, `key` | Which row |
| `from`, `to` | Row hash before and after; empty when absent |
| `targets` | Nodes that still need it (used by multi; the mesh tracks acknowledgements instead) |
| `sunset` | When the change dies |
| `txn` | `{id, count}` when the source wrote several rows as one transaction; applied all or none |
| `key_id` | Which shared key signed it |
| `stamp` | The table's shape stamp, a 16-byte hash of its column names, types and order |
| `signature` | Over every field above but the values; also the change hash the record names the change by |

The row's values follow the signature. A receiver refuses an envelope whose signature fails under every key it holds, whose values do not hash to `to`, or whose table has a different shape here.

## The datagram

Each message is JSON, sealed with XChaCha20-Poly1305 under the sealing key, with a fresh random 24-byte nonce carried before the ciphertext. Nothing passes in the clear: a capture of the traffic holds no row value and no key. A datagram that does not open under a key the node holds is not for it; the record says so at most once a minute, naming the count and the port (another mesh on the port, or a peer holding another key).

A message goes as one datagram and is never fragmented. An acknowledgement names at most 14 changes so that it stays under about 1,200 bytes. A change whose envelope and values do not fit the system's largest datagram is refused loudly: the node prints the table and key, says to catch it up with `smugglr stash` and `smugglr retrieve`, and drops it from the outbox so it does not hold up the changes after it. The limit is the system's: on macOS it is about 9 KB, `net.inet.udp.maxdgram`.

## Messages

| Message | From, to | Carries |
|---|---|---|
| announce | any node, by broadcast and to each address in `peers` | its name |
| change | maker, unicast to each peer; or a node echoing it, by broadcast | one envelope, and whether it is a resend |
| ack | receiver to maker, unicast | the maker's name and up to 14 change hashes |
| wazzup | waking node | its last-seen time, and a batch of `(key, to)` for each of its own changes a peer has not acknowledged. Not built (#521) |
| wazzup answer | each peer to the waking node | what changed since, or the too-much status; one line per listed key. Not built (#521) |

## Finding peers

Broadcast is for finding peers and unicast is for talking to them. A node announces itself when it starts and every 10 seconds, by broadcast and to each address in its `peers` setting. A node that hears a peer for the first time, or at a new address, announces itself back to it.

A node learns a peer's address from the source of its datagrams, never from its name. There is no member list, no discovery service and nothing to join: the shared key decides whether a datagram is for you.

On a network that carries no broadcast (a layer-3 VPN such as WireGuard or Tailscale, a cloud network) the `peers` setting is how nodes find each other. Name each peer in the other's config. Two nodes on one machine on different ports do the same with loopback addresses.

## Names

A node is named by its machine's name, as the `hostname` command gives it, or `COMPUTERNAME` on Windows. Every envelope it makes carries the name as its node. Renaming a machine makes it a new node to its peers.

In 0.6.1 a node treats any message carrying its own name as its own and ignores it. Two machines with one name therefore do not sync with each other, and nothing says so. The checks that make the clash loud (a starting node refuses a taken name; a node that hears a change under its own name that it did not make stops and names the clash) are specified in FR-SMUGGLR-079 and are not built (#521). Give every node its own name.

## Sending

- **Finding changes.** A node finds its own changes by comparing each row's hash with its last hash per key, so a row written by any program is sent. A node's first run notes the hashes of the rows already in its database and sends none of them; rows a peer lacks reach it by stash then retrieve.
- **First sends.** A change's first send goes unicast to every peer the node knows. A peer on this machine and port, which shares the node's socket group, is sent to by broadcast, because a unicast reaches only one socket there.
- **Pacing.** First sends are held to a window that starts at 32 changes, grows with each change a peer acknowledges, and holds at most 1,000. The window shrinks only on congestion: a full send buffer, or acknowledgements slowing. It never shrinks because a datagram was lost, and a lost first copy is repaired by a resend instead. Changes leave the outbox in bursts that start small and grow, so no burst overflows a socket buffer (RFC 8085).
- **Fast resend.** The maker resends, unicast, to a peer that is acknowledging but has not acknowledged a change. A change the peer skipped while acknowledging changes sent after it is resent at once, as in TCP's fast retransmit. Otherwise the first resend comes after about two of that peer's measured round trips, held between 50 ms and 300 ms however the round trips vary. Known losses go first: a change a peer skipped, or one the peer has never had, before a guess.
- **Resends never crowd out first sends.** While first sends wait, resends take at most a quarter of what the node sends in any moment. When no first send is left, up to 100 resends a tick may go past the quarter; when the window is held by changes waiting on repair, up to 10, so the quarter can never stop repair altogether.
- **Silent peers.** A peer that has stopped acknowledging is resent to on a doubling interval, from 10 seconds to at most 160 seconds, until it acknowledges or the sunset passes. This is for sleepers, so a node that went to sleep does not take the network's time.
- **Refill.** A node refills its outbox from its database before its first sends run dry, so a burst's last change still has changes sent after it to show a gap.
- **Echo with suppression.** A node that applies a change waits a random 2 to 4 seconds, then rebroadcasts it once, unless it has already heard it k more times in that wait (k is 2). This is the suppression rule of Trickle (RFC 6206). Echoes are paced, about 200 a moment, and when more than 20,000 are waiting the oldest is not sent: an echo is redundancy, and a change it would have carried still reaches every target by its maker's sends and resends.

## Acknowledgements

A node acknowledges a change once it has decided it: applied, skipped as a repeat, or stopped or dropped and named in its record. An acknowledgement means received and decided, not applied. A stopped change is held for a person, so resending it changes nothing, and its maker stops resending it.

A node acknowledges a repeat only when the copy came from the maker, and a first apply always, so echoes do not set off a storm of acknowledgements.

## Backpressure

The outbox holds the changes found but not yet acknowledged. It lives in smugglr's own files, never in the user's database and never all in memory, and it survives the node being stopped and started. It holds at most the `backpressure` setting. At that line the node takes no new changes into it until acknowledgements bring it down, and leaves them in the user's database to be found again by their hashes.

A node applies the changes it receives in one transaction per burst, at most 20,000 changes each, the chunk a direct sync uses.

## Receiving a change

A node decides each envelope in this order, the same order `smugglr-core` uses for every sync type:

1. The key is in the graveyard: a delete is a repeat (done); an edit whose `from` is the tombstone's `from` is an edit-versus-delete conflict; anything else is dropped and named in the record under dropped (a row re-created with the same key inside the window stays only where it was made).
2. The row is already at `to`: a repeat, done.
3. The key's last applied `from` equals this change's `from` and the row has moved: a conflict.
4. The node has its own unacknowledged change on the key with the same `from` and a different `to`: a conflict.
5. The row is at `from`: apply. A delete leaves a tombstone.
6. The change's `to` is the `from` of the last change applied to this key: the row has already moved past it, so it is skipped, not held (current choice 2026-10-22: a late copy delivered after the row moved on otherwise waited forever).
7. Otherwise hold it until its `from` appears or its sunset passes. A held change dropped at its sunset is named in the record.

A conflict follows `--sync-mode`: `stop` (the default) names the row; `last` keeps the copy whose steering column is further on.

A change past its sunset is ignored on arrival.

## The dropped log and replay

Every change a node drops is kept, envelope and values, in a dropped log in smugglr's own files, and named in the record with its table, key, reason and change hash. A change is dropped by step 1 of the order above (its key is in the graveyard and it is neither a delete nor the edit that step answers), or when it waited past its sunset.

`smugglr mesh <place> --key-file <path> --replay <change hash>` decides that logged change again on this node, with a fresh sunset, overriding the drop. It prints what came of it and exits. It is then decided like any change: an edit to a row deleted elsewhere stops as edit-versus-delete or follows `--sync-mode`, and a replayed insert for a key whose tombstone is still live is applied, since the replay overrides the drop. What the replay writes reaches the other nodes when the node next runs. A replay that still waits for its `from` stays in the dropped log. An unknown hash is a usage error, exit 2. `smugglr retrieve <place> --key-file <path> --node <name> --replay <change hash>` does the same for a change a retrieve dropped. Replay adds no rule; it only decides again.

## The record

A node prints one line for each of these, to its standard output: it started (node, port and the `key_id` it signs with); a burst applied, with the count, the table, the makers and the keys; a change holding, with why; a change stopped or dropped, once however often a copy arrives; a send window halved, with why; changes that arrived only as a resend because the first copy was lost; changes resent that a peer had not acknowledged, at most once a minute; a row too large for one datagram; and a datagram that did not open. A change refused for a key the node does not hold, or for a table whose shape differs here, goes to standard error.

## Waking

Every run a node notes when it last ran. A node that starts after being away longer than the sunset prints how long it was away and how to catch up, naming both commands, and keeps running for new changes:

    smugglr mesh: this node has been away 3 days, longer than the sunset (12 hours); changes made meanwhile may not reach it. Catch up with: smugglr stash <a peer's place> <dir> --key-file <path> --node <name> --whole, then smugglr retrieve ./node.db <dir> --key-file <path> --node <name>

That is all a waking node does in 0.6.1. Past the sunset the changes it missed are dead, so the person catches up with stash then retrieve.

**Specified, not built (#521).** FR-SMUGGLR-079 makes the catch-up automatic:

- A node that wakes says wazzup with its last-seen time, and its peers answer with what changed since, or, when that is over the `mesh_wire` setting (8 MB when not set, apart from `max_wire`), with a status code for too much, after which the node catches up by stash then retrieve.
- A node whose last-seen time is past the sunset does not ask for changes. The first awake, up-to-date node that hears it makes the stash, the sleeper retrieves it, then says wazzup with the stash's time to pick up what changed meanwhile.
- The wazzup carries a batch of `(key, to)` for each of the node's own changes a peer has not acknowledged, and each answer carries, per listed key, the peer's current hash and last applied `from`. From the answers the node settles each change peer by peer: a peer at the change's `to` has it; a peer at its `from` that is behind is sent the change with a fresh sunset; a peer whose last applied `from` equals the change's `to` built on it; anything else stays named in the record, grouped by peer, and nothing stops. Nothing past its sunset is resent blind: only this check, which has the peer's answer, sends one.
- A batch is split into numbered parts that each fit one datagram, each answered and matched by its id and resent until answered.

## Leaving

Any node can leave and the rest go on. A node that stops acknowledging is resent to on the 10 to 160 second doubling until the sunset. A node gone for good stays a target, and its peers keep its changes, until a person runs `smugglr mesh remove <node>`. That command is specified and not built (#521): in 0.6.1 `smugglr mesh remove` is refused as a usage error, and a gone node stops being a target only when the sunset passes.

## Clocks

Clocks matter only for sunsets and `--sync-mode last`. They should agree to within one twelfth of the sunset: an hour at the 12-hour default. The echo wait, the resend intervals and the window run on each node's own clock and are never compared across nodes.

## Evidence

Research documents in the team's records, cited by id; the toys behind them are kept outside this repository:

| Toy | What it shows |
|---|---|
| L, M, N, O (01a141fa, 01a14214, 01a1421a, 01a14220) | the envelope, `from` and `to`, the graveyard, sunsets |
| Q (01a14c18) | third-place conflicts and edit versus delete stop and are named |
| S (01a14c20) | every catch-up loss is the sleeper's own last change, resent only by its maker |
| X (01a14d48) | stash then name: no loss is silent; echo plus resend leaves almost nothing to name |
| Z (01a14d86) | Trickle-suppressed echo and doubling resends at 5, 20 and 50 nodes; the wazzup check |
| Y (01a14d93) | the decision path is the same function for direct sync and the mesh |
| AA (01a15091) | why the mesh is not pairwise direct syncs: deletes come back after a long sleep, and every sync reads every key |

## Prior art

| Idea here | From |
|---|---|
| echo, and repair by whoever holds the data | Demers et al. 1987 (epidemic algorithms); SRM, Floyd et al. 1995 |
| suppression after k copies heard | Trickle, RFC 6206 (and practice in multicast streaming long before it) |
| unicast to known peers, broadcast to find them | DDS/RTPS, whose Wi-Fi guidance is unicast to known peers |
| resend of a skipped change at once | TCP fast retransmit |
| a send window that shrinks on congestion, not on loss | RFC 8085 on congestion control for UDP |
| retry with a doubling timeout | CoAP confirmable messages, RFC 7252 |
| the too-much status and falling back to a bulk path | DNS truncation and retry, RFC 1035 |
| numbered parts and per-part resend | TFTP, RFC 1350 |
| matching a reply to its request by id | Sun RPC, RFC 1057 |
| datagram size | RFC 8085; IPv6's 1,280-byte minimum MTU |
