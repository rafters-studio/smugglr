# cli-lan-broadcast

Machines on the same LAN keep their SQLite databases in sync over UDP multicast. No cloud, no relay, no coordinator: every node runs the identical loop, multicasts a `primary_key -> content_hash` digest of its tables each interval, and requests the rows it lacks. Whoever holds them answers on the same group, and every listener applies them idempotently, so one answer converges the whole group.

## Prerequisites

`smugglr` 0.5.0 on every machine, all machines on one subnet (multicast does not cross routers or VLANs), a shared 256-bit key, and UDP open on the chosen port. On macOS the application firewall silently drops inbound multicast for a binary it has not been told about; allow `smugglr` when prompted, or the node hears nothing and logs nothing about it.

## Setup

Generate the key once and distribute it out of band.

```sh
openssl rand -hex 32
```

On each machine, copy `config.example.toml` to `config.toml`, paste the same key into `[broadcast].secret`, use the same `port`, and point `local_db` at that machine's database. Paths need not match across machines. Then create the tables on each machine; smugglr syncs rows, not schema, and every synced table needs a globally unique primary key.

```sh
sqlite3 ./node.db "CREATE TABLE items (id TEXT PRIMARY KEY NOT NULL, name TEXT, updated_at INTEGER NOT NULL);"
```

`config.example.toml` (the key and the conflict policy are the parts that matter):

```toml
local_db = "./node.db"

[sync]
tables = []
timestamp_column = "updated_at"

[broadcast]
secret = "REPLACE_WITH_OUTPUT_OF_openssl_rand_-hex_32"
port = 31337
interval_secs = 5
conflict_resolution = "newer_wins"
ordering_columns = ["updated_at"]
```

## Run

On each machine:

```sh
smugglr broadcast
```

`broadcast` writes nothing to stdout in text mode; everything it says is on stderr. Captured from one node on 2026-08-28 (timestamps and ANSI color stripped):

```
$ smugglr broadcast
 INFO Acquired PID lock: .smugglr-broadcast.pid (PID 30382)
 INFO Starting masterless multicast sync (group 239.255.43.21, port 31999, interval 2s, instance peer-d, dry_run false)
 INFO Opening local database: d.db
 INFO Heartbeat #1: multicast 1 digest datagram(s)
 INFO Heartbeat #2: multicast 1 digest datagram(s)
```

`--dry-run` advertises and applies nothing; `--once` sends one heartbeat, waits briefly for answers, and exits. `-v` shows every table read and every datagram handled.

## Two peers on one machine

Useful for a first test, and it fails in two ways that are not documented anywhere else. Each node writes `.smugglr-broadcast.pid` in its working directory, so two nodes started from the same directory refuse with `Another smuggler watch instance is running`; run each from its own directory. And `instance_id` defaults to the hostname, and a node drops every datagram carrying its own id, so two nodes on one host ignore each other until each config sets a distinct `[broadcast] instance_id`. With both fixed, the run on 2026-08-28 still converged nothing on a Mac with the application firewall on; it was not verified here across two machines. Run it on two, or allow the binary through the firewall, before drawing conclusions from a single-host test.

## What to expect across machines

A row inserted on A is requested by B on B's next heartbeat and applied there. A late joiner that starts empty hears the next digest and converges with no special step. A node that drops off, takes writes, and rejoins re-converges on the next heartbeat. Lost packets are safe: applying a row twice is a no-op, and the next heartbeat re-reconciles.

## What this demonstrates

Masterless, peer-symmetric sync: no primary, no leader, no server, no internet. Membership by key: with `secret` set, all traffic is XChaCha20-Poly1305 with a fresh nonce per datagram, and a node with a different key sees only ciphertext it cannot open. Leave `secret` out and the node still runs, in the clear, accepting any well-formed datagram from the subnet; it warns once at start. This is for networks you trust; on anything else, run it inside a tunnel.

Conflict policy is apply-side and per node. `remote_wins`, the default, is last-received-wins: the row that arrives last replaces the local one, evaluated on each node, so two nodes that receive two writes in different orders can hold different rows for that key. `newer_wins` takes the row whose `ordering_columns` maximum is greater, which is the only policy that converges under concurrent edits, and both nodes must opt in. Deletes do not replicate on any path in 0.5.0; model a delete as a `deleted_at` column, which rides the upsert path and converges.

`exclude_columns` keeps columns out of the digest, so an edit confined to them does not churn the mesh; in 0.5.0 those columns still ride along when a peer requests the row (#322).
