# cli-stash-file-relay

Two machines that cannot reach each other share state through a relay: a SQLite file in an object store. Machine A stashes, machine B retrieves. This example runs both roles on one machine through a `file://` URL, so the whole S3 workflow is visible with no bucket and no credentials.

## Prerequisites

`smugglr` 0.5.0 on your PATH, the `sqlite3` shell, and the [westwind](../westwind/) sample in this repository. For a real deployment, an S3-compatible bucket (S3, R2, MinIO) and its keys.

## Setup

Machine A gets the full sample; machine B gets the same schema with no rows, built by the same eight migrations. One config per machine, differing only in `local_db`; the relay path is the same in both.

```sh
mkdir -p /tmp/smugglr-relay
../westwind/make.sh ./machine-a.db
../westwind/make.sh --empty ./machine-b.db
cp config.example.toml config-a.toml
sed 's/machine-a.db/machine-b.db/' config.example.toml > config-b.toml
```

`config.example.toml`:

```toml
local_db = "./machine-a.db"

[stash]
url = "file:///tmp/smugglr-relay/relay.sqlite"

[sync]
timestamp_column = "updated_at"
conflict_resolution = "newer_wins"
```

For S3 or R2, replace `url` with `s3://bucket/path/relay.sqlite` and add `access_key_id`, `secret_access_key`, `region` (default `us-east-1`), and, for R2 or MinIO, `endpoint`. The key names a file, not a prefix.

## Run

Machine A stashes. On first run the relay does not exist, so smugglr creates it from A's schema, diffs A against it, and uploads it.

```
$ smugglr -c config-a.toml stash

--- Stash Summary ---
  categories: 8 rows
  customers: 40 rows
  employees: 9 rows
  order_details: 788 rows
  orders: 320 rows
  products: 20 rows
  shippers: 3 rows
  suppliers: 8 rows
```

Machine B retrieves. smugglr downloads the relay, diffs it against B, and applies the rows B lacks.

```
$ smugglr -c config-b.toml retrieve

--- Retrieve Summary ---
  categories: 8 rows
  customers: 40 rows
  employees: 9 rows
  order_details: 788 rows
  orders: 320 rows
  products: 20 rows
  shippers: 3 rows
  suppliers: 8 rows
```

A second retrieve finds nothing to do, because B's content hashes now match the relay's.

```
$ smugglr -c config-b.toml retrieve

--- Retrieve Summary ---
  No changes to retrieve
```

The rows arrived.

```
$ sqlite3 machine-b.db "select code, company from customers order by code limit 3"
ARBOR|House Redwyne of the Arbor
BEARI|House Mormont of Bear Island
BLKWT|Blackwater Rush, East Bank
```

## What the relay is

The relay is a SQLite file the diff engine syncs row by row, in both commands. `stash` downloads it, upserts local rows into it, and uploads it back; `retrieve` downloads it and upserts its rows into the local database. The local file is never replaced wholesale. On S3, the upload is conditional on the ETag read at download, so two machines stashing at once cannot overwrite each other: the loser exits 4 (`conflict`) and runs again. On `file://` there is no ETag, so the put is unconditional and smugglr warns once.

Two limits worth knowing before pointing this at a bucket. `exclude_columns` is not applied on this path in 0.5.0, so a column excluded to keep it off the wire still reaches the relay (#322). And the relay's schema is copied from the first machine to stash, so every machine needs the same tables.

## What this demonstrates

Cross-machine sync with nothing listening: no server, no daemon, an object store as the only shared thing. The same diff engine and the same `--- Summary ---` block as the direct paths. And idempotence: a retrieve that finds nothing new says so and writes nothing.

Every block on this page was captured from smugglr 0.5.0 on 2026-08-28 by running the commands shown, in this directory, in this order, against the committed Westwind seed.
