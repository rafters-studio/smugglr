# The smugglr mesh protocol

Status: draft for 0.6, 2026-10-18. Current choice of the design team (Sean with smugglr): smugglr carries its own mesh protocol rather than a gossip or membership library, and documents it here for its own code and for programs that use the crates. Revisable.

`smugglr mesh` keeps the copies of one SQLite database in step across machines on a local network where nobody is in charge: no server, no hub, no coordinator. Any node can leave and the rest go on. This document says what goes over the wire and what each node does with it. The rules that decide what a change means (apply, skip, hold, conflict, graveyard) belong to `smugglr-core` and are the same for every sync type; the mesh only carries them.

Scope: designed and tested for up to 50 nodes. Hundreds of nodes is a different problem and is outside 0.6.

## Terms

- **Node**: one running `smugglr mesh` process with its database.
- **Change**: one row's move from one hash to another: `{op, table, key, from, to, values}`. `from` is the row's hash before the change, `to` after; the empty hash means the row is absent.
- **Envelope**: a Change plus what the mesh needs to carry it (below).
- **Target**: a node that has not yet acknowledged a change.
- **Sunset**: the time after which a change is dead. Set by its sender: the sender's clock plus the graveyard setting (12 hours unless set).
- **Graveyard**: every delete envelope a node has applied, kept until its sunset.
- **Shared key**: the one key every node holds, given with `--key-file`.

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
| shape stamp | The table's migratr ledger version and checksum, or an 8-byte hash of its columns |

A signature over the envelope follows it, then the row's values. The whole datagram is encrypted with the shared key, so no row value or key crosses the network in the clear. A receiver refuses an envelope whose signature fails or whose values do not hash to `to`.

A node signs with the newest key it holds and accepts the previous key until that key's last change passes its sunset.

## Messages

| Message | From, to | Carries |
|---|---|---|
| change | maker or echoing node, broadcast | one envelope |
| ack | receiver to maker | the change's id |
| wazzup | waking node, broadcast | its last-seen time, and a batch of `(key, to)` for each of its own changes a peer has not acknowledged |
| wazzup answer | each peer to the waking node | what changed since that time, or the too-much status; one line per listed key: its current hash and last applied `from` |

Every datagram stays under about 1,200 bytes so it is never fragmented. A wazzup batch larger than that goes as numbered parts; each part is answered and matched by its id and resent until answered. A batch over `max_wire` (100 MB unless set) is answered with the too-much status.

## Receiving a change

A node decides each envelope in this order, the same order `smugglr-core` uses for every sync type:

1. The key is in the graveyard: a delete is a repeat (done); an edit whose `from` is the tombstone's `from` is an edit-versus-delete conflict; anything else is dropped and named in the record under dropped (a row re-created with the same key inside the window stays only where it was made).
2. The row is already at `to`: a repeat, done.
3. The key's last applied `from` equals this change's `from` and the row has moved: a conflict.
4. The node has its own unacknowledged change on the key with the same `from` and a different `to`: a conflict.
5. The row is at `from`: apply. A delete leaves a tombstone.
6. Otherwise hold it until its `from` appears or its sunset passes. A held change dropped at its sunset is named in the record.

A conflict follows `--sync-mode`: `stop` (the default) names the row; `last` keeps the copy whose steering column is further on. `source` and `target` are for two places and are a usage error in the mesh.

A change past its sunset is ignored on arrival, except one the wazzup check sends to a peer that is behind, which carries a fresh sunset.

## Sending

- A change goes out once, as a broadcast on the local network.
- **Echo with suppression.** A node that applies a change waits a random 2 to 4 seconds, then rebroadcasts it once, unless it has already heard it k more times in that wait (k is 2 unless set). This is the suppression rule of Trickle (RFC 6206).
- **Acknowledgements.** A node acknowledges a change when it applies it, and acknowledges a repeat only when the copy came from the maker, so echoes do not set off a storm of acks.
- **Resend.** The maker resends to targets that have not acknowledged, with the interval doubling from 10 seconds up to 160 seconds, until every target has or the sunset passes.

## Waking

A node that wakes says wazzup with its last-seen time.

- **Inside the sunset**, peers answer with what changed since. When that is more than `max_wire`, they answer with the too-much status and the node catches up by stash then retrieve.
- **Past the sunset**, the node does not ask for changes. The first awake, up-to-date node that hears it makes a stash; the waking node retrieves it, then says wazzup with the stash's time.

At the end of the wazzup the waking node settles each of its own unacknowledged changes from the answers, peer by peer:

1. The peer is at the change's `to`: it has it.
2. The peer is at the change's `from` and is behind (for an insert, it never saw the key): send it the change.
3. The peer's last applied `from` equals the change's `to`: it built on it.
4. Anything else stays named in the record, grouped by peer, and stops nothing.

Nothing past its sunset is resent blind. A row left named stays different until it is changed again or its change is replayed.

## The dropped log and replay

Every change a node drops is kept, envelope and values, in a dropped log in smugglr's own files: a change arriving past its sunset, a change that waited past its sunset, an insert or waiting change for a key in the graveyard, and every own change the wazzup check leaves named. The record names each with its table, key, reason and change hash.

`smugglr mesh ./node.db --key-file ./mesh.key --replay <change hash>` re-sends that change with a fresh sunset, overriding the drop. It is then decided like any change: an edit to a row deleted elsewhere stops as edit-versus-delete or follows `--sync-mode`; an insert dropped by the graveyard lands once the window has passed; an own change reaches the peer that lacked it. Replay adds no rule; it only sends again.

## Leaving

A node gone for good stays a target, and its peers keep its changes, until a person runs `smugglr mesh remove <node>`.

## Clocks

Clocks matter only for sunsets and `--sync-mode last`. They should agree to within one twelfth of the sunset: an hour at the 12-hour default. The echo wait and the resend doubling run on each node's own clock and are never compared across nodes.

## Membership

There is no discovery and no member list to join. A node broadcasts; every node on the local network hears it; the shared key decides whether a datagram is for you: one that does not decrypt and verify under a key you hold is not yours, and is dropped. A node's peers are the nodes it hears.

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
| retry with a doubling timeout | CoAP confirmable messages, RFC 7252 |
| the too-much status and falling back to a bulk path | DNS truncation and retry, RFC 1035 |
| numbered parts and per-part resend | TFTP, RFC 1350 |
| matching a reply to its request by id | Sun RPC, RFC 1057 |
| datagram size | RFC 8085; IPv6's 1,280-byte minimum MTU |
