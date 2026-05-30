# Masterless multicast anti-entropy sync (v0.1)

**Status:** building
**Spec source of truth:** `vault/site/homepage.md` -> "LAN broadcast semantics"
(the 7 bullets, reproduced below). The smugglr.dev copy is the contract; the
current implementation diverges from it and that divergence is the bug.

## The spec (canonical, do not paraphrase away)

- **Masterless.** No primary, secondary, coordinator, or leader election. Every
  node equal.
- **Peer-symmetric.** Every node is both broadcaster and listener. No
  client/server distinction.
- **Membership = key possession.** The shared encryption key IS access control.
  Have the key, you are on the network. Key rotation is how a machine leaves.
- **Idempotent apply.** Incoming row compared to local content hash: new/different
  -> upsert, identical -> skip. Apply-twice is a no-op, so **lost packets are safe**.
- **Last-received-wins on divergence.** Same PK, different contents -> receiver
  replaces local with incoming. UUIDv7 PKs make concurrent divergence rare. **No
  vector clocks, no CRDTs.**
- **Heartbeat reconciliation.** Every node periodically broadcasts a
  **primary_key -> content_hash map for all rows it holds**. Peers compare against
  local state and **pull any missing or differing rows**. Covers late joiners,
  missed packets, partition rejoin.
- **Encryption.** XChaCha20-Poly1305 AEAD, shared 256-bit key, unique per-datagram
  nonce. User-managed key; smugglr reads it, never generates or stores it.

The load-bearing consequence: "idempotent apply", "lost packets are safe", and
"last-RECEIVED-wins" only make sense if **row data rides lossy UDP multicast**.
TCP would make loss impossible and reception order deterministic, so those bullets
would be meaningless. **The transport is UDP multicast, end to end. No TCP.**

## Two shapes, not one transport (the correction)

smugglr has multiple sync **shapes**; this work is about exactly one of them.

- **LAN broadcast shape** (`smugglr broadcast`): masterless multicast gossip,
  built here. Scales to 2 or 200 nodes on a subnet with no coordinator -- one
  multicast send reaches every peer, so digest fan-out and row convergence are
  O(N), not the O(N^2) of pairwise connections.
- **Cross-process / cross-subnet shape** (#90 TCP framing envelope,
  `handle_sync_connection`/`sync_with_peer`/`write_framed`): **retained, not
  touched.** Multicast does not cross routers; TCP is how an embedder (e.g.
  legion) bridges processes or subnets the multicast fabric cannot reach.

The bug was never "TCP exists." It was that the LAN broadcast shape *claimed*
masterless multicast but actually did **client-initiated pairwise TCP**
(`run_broadcast_once` dumping full pk->hash maps to every peer) -- which cannot
auto-sync 200 nodes. That pairwise-over-TCP behavior in the LAN daemon is what
the multicast gossip replaces. The TCP primitives stay in `smugglr-core` for the
cross-process shape.

## Why the LAN fabric must be pure multicast (rows included)

"Two or two hundred nodes all automatically synced" forces it: a single multicast
datagram reaches the whole group, so a digest or a served row converges every
peer at once. This is also why the spec insists on idempotent apply,
lost-packets-safe, and last-received-wins -- those properties are only meaningful
because the **rows themselves ride lossy UDP multicast**, not a reliable stream.

## Target protocol (UDP multicast only)

One multicast group (default `239.255.43.21:<port>`, configurable), all nodes
join, all datagrams `maybe_encrypt`-wrapped with the shared key. Tagged envelope:

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "t")]
enum Msg {
    Digest(DigestPacket), // pk -> content_hash map for a table (heartbeat)
    Want(WantPacket),     // table + pks this node is missing/divergent on
    Delta(DeltaPacket),   // rows (upserts + deletes); already exists
}
```

Bumps wire `PROTOCOL_VERSION` 1 -> 2 (the UDP payload shape changes). v1 nodes
(legion's pinned v0.2.1, #536) version-skip until they upgrade. Inherent to any
real protocol here -- flagged to legion, noted in CHANGELOG.

### Loops (identical on every node -- peer-symmetric)

**Emit (heartbeat, every `interval_secs`):** for each syncable table, build
pk->content_hash via `LocalDb::get_row_metadata`, chunk to `SAFE_PACKET_SIZE`
(reuse the `split_delta` sizing), multicast each chunk as `Digest`.

**Emit (live write):** on a local change, multicast the changed rows as `Delta`
immediately (optimistic). Heartbeat reconciles anything lost.

**Receive `Digest`:** reassemble peer chunks (replay-guard on source_id+seq).
Compute want-set = pks the peer advertises that we **lack or hash-differ** on.
Non-empty -> multicast `Want(table, pks)`.

**Receive `Want`:** for pks we hold, multicast `Delta` with those rows. Multicast
(not unicast) so every peer converges from one answer -- no client/server.

**Receive `Delta`:** idempotent last-received-wins apply -- compare incoming row
content hash to local; new/different -> upsert (replace), identical -> skip.
Deletes applied from `Delta.deletes`.

**Late joiner / partition rejoin:** empty/stale node hears the next `Digest`,
every advertised pk diverges, it `Want`s and converges. Falls out of the loop, no
special case.

## What we reuse vs build

Reuse (already present + tested): `DeltaPacket`, `split_delta`/`reassemble_delta`,
`SequenceTracker`, `ReplayGuard`, `maybe_encrypt`/`maybe_decrypt`,
`get_row_metadata`, `upsert_rows`.

Build: multicast group join (replace 255.255.255.255 broadcast); `Msg` envelope +
`DigestPacket`/`WantPacket`; the digest-chunk + want-set computation; the single
multicast send/recv loop; idempotent last-received-wins apply path; wire into the
daemon (`crates/smugglr/src/broadcast.rs`, the `run_broadcast_once` callers at
:8 and :125 per `legion sym refs`).

Keep, untouched: the TCP path -- `handle_sync_connection`, `sync_with_peer`,
`write_framed`/`read_framed`, `SyncRequest`/`SyncResponse`. It is the
cross-process/cross-subnet shape (#90), not part of the LAN fabric. The only LAN
change is that `smugglr broadcast` stops doing pairwise-TCP and runs multicast
gossip instead.

## v0.1 limits (documented, not hidden)

- **Deletes** propagate via the live `Delta.deletes` path only. The heartbeat
  digest advertises presence, so it reconciles upserts/divergence, NOT deletions
  (an absent pk is indistinguishable from not-yet-received). Tombstone propagation
  = v0.2. Stated in README.
- **Large tables** re-advertise the full pk->hash map each heartbeat. Merkle /
  bucketed digests to skip unchanged state = v0.2.
- **Unencrypted mode** exposes pk+hash+rows on the group (same as today's deltas).
  Membership=key means running without a key = no membership control; warn.

## Test plan (must pass before "done" -- no multi-machine claim)

- Unit: `Msg` tag round-trip; `DigestPacket` split/reassemble; want-set
  (lack/differ/identical); v1 version-skip; idempotent apply (apply-twice no-op);
  last-received-wins replace.
- Integration (loopback multicast, two `LocalDb`s on one group): A has rows B
  lacks -> B converges within N heartbeats; mutate A row -> B converges; B joins
  empty -> converges; drop/duplicate a Delta -> still converges (idempotency).
- Multi-machine (cross-subnet, real Wi-Fi) needs Sean's hardware -- out of CI,
  stated plainly, not implied.
