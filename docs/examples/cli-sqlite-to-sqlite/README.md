# cli-sqlite-to-sqlite

Two SQLite files on one machine. The same diff engine that drives every hosted backend runs between them, so this is the shortest path to seeing what smugglr prints. No credentials, no network, no plugin.

## Prerequisites

`smugglr` 0.5.0 on your PATH (`cargo install smugglr`, or the release binary), the `sqlite3` shell, and the [westwind](../westwind/) sample in this repository.

## Setup

Build the sample as the local database and the same schema with no rows as the target. smugglr moves rows, not DDL, so the target must already have the tables; `make.sh` applies Westwind's eight migrations to both.

```sh
cp config.example.toml config.toml
../westwind/make.sh ./local.db
../westwind/make.sh --empty ./backup.db
```

`config.toml` is the whole configuration:

```toml
local_db = "./local.db"

[target]
type = "sqlite"
database = "./backup.db"

[sync]
tables = []
timestamp_column = "updated_at"
conflict_resolution = "local_wins"
```

## Run

A dry-run reads both sides, hashes every row, and reports what a push would write. Nothing is written.

```
$ smugglr push --dry-run
--- Push Summary ---
  order_details: 788 rows
  suppliers: 8 rows
  employees: 9 rows
  shippers: 3 rows
  categories: 8 rows
  products: 20 rows
  customers: 40 rows
  orders: 320 rows

  (dry run - no actual changes made)
```

The push writes exactly what the dry-run described. Same counts; the lines come out in a different order because the summary iterates a map, not a list, so compare counts and not line positions.

```
$ smugglr push
--- Push Summary ---
  suppliers: 8 rows
  products: 20 rows
  employees: 9 rows
  customers: 40 rows
  order_details: 788 rows
  categories: 8 rows
  orders: 320 rows
  shippers: 3 rows
```

A second push has nothing to move, because every content hash now matches.

```
$ smugglr push

--- Push Summary ---
  No changes to push
```

`diff` shows the bucket each row landed in. After a push, everything is `identical`.

```
$ smugglr diff
--- Differences ---

shippers: in sync (3 rows)

products: in sync (20 rows)

customers: in sync (40 rows)

suppliers: in sync (8 rows)

categories: in sync (8 rows)

orders: in sync (320 rows)

employees: in sync (9 rows)

order_details: in sync (788 rows)

All tables are in sync!
```

## Output for agents

`--output json` is a global flag, so it goes before the command. It silences the tracing on stderr and prints one JSON object on stdout. Captured after touching six orders locally:

```
$ sqlite3 local.db "UPDATE orders SET freight = freight + 5, updated_at = updated_at + 60 WHERE ship_city = 'Dragonstone'"
$ smugglr --output json push --dry-run
{"command":"push","status":"dry_run","tables":[{"name":"suppliers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":8,"rows_to_push":0,"rows_to_pull":0},{"name":"categories","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":8,"rows_to_push":0,"rows_to_pull":0},{"name":"orders","local_only":0,"remote_only":0,"local_newer":6,"remote_newer":0,"content_differs":0,"identical":314,"rows_to_push":6,"rows_to_pull":0},{"name":"shippers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":3,"rows_to_push":0,"rows_to_pull":0},{"name":"order_details","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":788,"rows_to_push":0,"rows_to_pull":0},{"name":"customers","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":40,"rows_to_push":0,"rows_to_pull":0},{"name":"employees","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":9,"rows_to_push":0,"rows_to_pull":0},{"name":"products","local_only":0,"remote_only":0,"local_newer":0,"remote_newer":0,"content_differs":0,"identical":20,"rows_to_push":0,"rows_to_pull":0}],"total_rows_to_push":6,"total_rows_to_pull":0,"exit_code":0}

$ smugglr --output json push
{"command":"push","status":"ok","tables":[{"name":"orders","rows_pushed":6}]}

$ smugglr --output json push
{"command":"push","status":"ok","tables":[]}
```

The dry-run object carries every table and every bucket; the push object carries only the tables that moved, so an empty `tables` is a no-op. The six rows are `local_newer`, not `content_differs`, because the update also bumped `updated_at`; an edit that leaves the timestamp alone lands in `content_differs` and is resolved by `conflict_resolution`. The exit code is the scripting contract: 0 success, 1 general error, 2 configuration error, 3 connection error, 4 conflict, 5 target not found.

## What the text mode also prints

Without `--output json`, smugglr writes timestamped `INFO` lines to stderr for every step, whether or not `-v` is given (`-v` raises the level to DEBUG). The blocks above are stdout only. The first three stderr lines of a dry-run look like this:

```
2026-08-28T22:08:11.303157Z  INFO Opening local database (read-only): ./local.db
2026-08-28T22:08:11.303347Z  INFO Push mode: local -> SQLite (./backup.db)
2026-08-28T22:08:11.303356Z  INFO Opening local database: ./backup.db
```

## What this demonstrates

Content-hashed delta: a second push after no edits moves nothing, and a change to six rows moves six rows out of twelve hundred. Dry-run and push parity in the counts. And the split between stdout and stderr that lets a script consume the summary or the JSON while a human watches the log.

Every block on this page was captured from smugglr 0.5.0 on 2026-08-28 by running the commands shown, in this directory, in this order, against the committed Westwind seed.
