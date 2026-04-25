# cli-lan-broadcast

Two laptops on the same Wi-Fi network keep their SQLite databases in sync via UDP broadcast. No cloud, no S3, no relay -- peers discover each other and exchange encrypted deltas directly.

## Prerequisites

- `smugglr` on both machines (same major version)
- Both machines on the same subnet (typically: same Wi-Fi)
- A shared 256-bit encryption key (a hex string both sides know)
- Each machine has its own copy of the SQLite database file at the same path-relative-to-config

## Setup

1. Generate a shared key once:

   ```sh
   openssl rand -hex 32
   ```

   Distribute this securely to both machines. Don't put it in git.

2. On each machine, copy `config.example.toml` to `config.toml` and:
   - paste the same key into `[broadcast].encryption_key`
   - point `local_db` at this machine's copy of the database
   - leave the rest at defaults

3. Make sure UDP traffic on the broadcast port (default `47291`) isn't blocked by firewall.

## Run

On each machine:

```sh
smugglr daemon
```

The daemon registers as a peer, listens for broadcast announcements from other smugglr peers, and exchanges deltas every time a row changes locally or arrives from a peer.

You can verify discovery with:

```sh
smugglr daemon --output json | jq '.peers'
```

## Expected behavior

- Insert a row on machine A. Within a second, the same row appears on machine B.
- Disconnect machine A from Wi-Fi, write more rows. Reconnect. Machine B catches up automatically; conflicts resolve per `conflict_resolution`.

## What this demonstrates

- Offline-tolerant peer-to-peer sync. No central server, no internet required.
- All broadcast payloads are encrypted with the shared key. Anyone on the LAN can see the announcement headers but not the row contents.
- `uuid_v7_wins` conflict resolution is the recommended setting for master-master topologies because UUIDv7 PKs encode creation time -- newer writes deterministically win without a clock-sync requirement.
