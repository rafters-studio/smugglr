# cli-lan-broadcast

Two (or two hundred) machines on the same LAN keep their SQLite databases in sync
via masterless UDP multicast. No cloud, no relay, no coordinator: every node runs
the identical loop, multicasts a `primary_key -> content_hash` digest of its
tables, and pulls any rows it is missing. Rows ride multicast and apply
idempotently, so one node's answer converges the whole group.

## Prerequisites

- `smugglr` built from a version with multicast sync (`smugglr broadcast` runs
  the masterless gossip loop). Build from source if your release predates it:
  `cargo build --release -p smugglr`.
- All machines on the same subnet (multicast does not cross routers/VLANs).
- A shared 256-bit key (one hex string every node knows).

## Setup

1. Generate the shared key once and distribute it securely (do not commit it):

   ```sh
   openssl rand -hex 32
   ```

2. On each machine, copy `config.example.toml` to `config.toml` and:
   - paste the same key into `[broadcast].secret` (this key IS the membership
     check -- nodes with it sync, nodes without it can't decrypt and are ignored),
   - set the same `[broadcast].port` on every node,
   - point `local_db` at this machine's database. **The path does not need to
     match across machines** -- sync is scoped by key, not by file location.

3. Create the table(s) you want synced on each machine (smugglr syncs rows, not
   schema -- the table must exist on both, with a PRIMARY KEY):

   ```sh
   sqlite3 ./node.db "CREATE TABLE items (id TEXT PRIMARY KEY, name TEXT, updated_at TEXT);"
   ```

4. Allow UDP on the broadcast port through the firewall (on macOS, click Allow
   when prompted on first run).

## Run

On each machine:

```sh
smugglr -c config.toml broadcast -v
```

You'll see it join the group and emit a heartbeat each interval:

```
Starting masterless multicast sync (group 239.255.43.21, port 31337, ...)
Heartbeat #1: multicast 1 digest datagram(s)
```

To preview what a node would advertise without sending or applying anything:

```sh
smugglr -c config.toml broadcast --dry-run -v
```

## Expected behavior

- Insert a row on machine A; within a few heartbeats it appears on machine B
  (B logs `Applied 1 row(s)`). The reverse works identically -- every node both
  broadcasts and listens.
- A late joiner that starts empty hears the next heartbeat and converges to the
  group's full state, no extra steps.
- Drop a machine off the network, write rows, reconnect: it re-converges on the
  next heartbeat. Lost packets are safe -- applying a row twice is a no-op.

## What this demonstrates

- **Masterless, peer-symmetric sync.** No primary, no leader, no central server,
  no internet required.
- **Membership = key possession.** The shared key is the only access control and
  the encryption key (XChaCha20-Poly1305, unique nonce per datagram). Run an
  isolated cluster on the same LAN by using a different key.
- **Last-received-wins on divergence.** Same primary key with different contents:
  the received row replaces the local one. UUIDv7 PKs make concurrent divergence
  on the same logical row rare; there are no CRDTs or vector clocks.
