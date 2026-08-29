# rust-tokio-service

Embed smugglr inside a long-running tokio service. The service holds a local SQLite database open, calls `sync_all` on a tick against an HTTP-SQL endpoint, and shuts down cleanly on `Ctrl-C`. It bypasses the CLI entirely, which is the right pattern for a backend that already owns its database connection.

## Prerequisites

Rust stable, the `sqlite3` shell, Python 3 for the stand-in endpoint, and a `smugglr` binary for the sample database's `make.sh` (`cargo build` at the repository root produces `target/debug/smugglr`; `make.sh` reads it from `SMUGGLR` or `$PATH`). The example depends on `smugglr-core` 0.5.0 by path into this repository's `crates/smugglr-core`; outside the repository, drop the `path` key from `Cargo.toml` and the same version resolves from crates.io.

The remote side is reached through the `smugglr-http-sql` plugin binary, the same one the CLI spawns. `cargo install smugglr-http-sql` puts it on `$PATH`, where `resolve_plugin_path("http-sql")` finds it; the run below instead points `SMUGGLR_HTTP_SQL_PLUGIN` at a workspace build from `cargo build --release -p smugglr-http-sql` at the repository root.

The endpoint must speak the `generic` profile: POST `{"sql": ..., "params": [...]}` in, `{"columns": [...], "rows": [[...]]}` out. `serve.py` in this directory is one, over a single SQLite file, using only the Python standard library. rqlite or your own gateway work the same way; set `HTTP_SQL_PROFILE` to `turso` or `rqlite`, with `HTTP_SQL_URL` and `HTTP_SQL_TOKEN`, to point the unchanged service at a hosted service. Nobody ran that here, so this README shows no output for it; it needs an account and a token for that provider.

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOCAL_DB` | `local.db` | SQLite file opened with `LocalDb::open` |
| `HTTP_SQL_URL` | `http://127.0.0.1:18787/sql` | endpoint the plugin posts to |
| `HTTP_SQL_PROFILE` | `generic` | request and response shape, a `Profile` name |
| `HTTP_SQL_TOKEN` | empty | bearer token, sent when set |
| `SYNC_INTERVAL` | `30` | seconds between ticks |
| `SMUGGLR_HTTP_SQL_PLUGIN` | unset | plugin binary; unset means the CLI's search of `~/.smugglr/plugins` then `$PATH` |

## Run

Build both sides from the shared Westwind sample in `../westwind/`: the local file with its seed rows, the remote file with the same eight tables and no rows. Both files are ignored by git.

```sh
SMUGGLR=/Volumes/store/projects/rafters-studio/smugglr/target/debug/smugglr ../westwind/make.sh ./local.db
```

```
Applied migration v1 (1 op) -- checksum f40fed16bdb497a97d42a748a217ff175950ddcd4f5d34d007e0464fc5323f5f
Applied migration v2 (1 op) -- checksum 846afb23867541c0aa5b1e51caf0422b3b6139fdd74be483053ee963ec0149a2
Applied migration v3 (2 ops) -- checksum d45e3062fb7d3ebfc9d2c1ddaf5b1bfa1f01832d8f7d5c68fa6608b8c3ed81c2
Applied migration v4 (1 op) -- checksum c6447ae278a2e42d6b1cbf73e7e262ceac42b352b9ab77e483597f229b3f5942
Applied migration v5 (1 op) -- checksum c0bef91533708c8d4e3fc5c40f80fb9c5ea5db05f67becd58a994ff814fcbc25
Applied migration v6 (1 op) -- checksum 78ef6eae8eac922753460c0261d06006446d1b951298b0c11b1b6d2171e2828e
Applied migration v7 (3 ops) -- checksum 4b5cce42abd1e797066f5bfbebf4a96a53f3e8ff62d3ab6d0ad91cc703cbd701
Applied migration v8 (2 ops) -- checksum 282301d633321a5785ba012e5a176443fdc21981cb5c690dce9f24e18809bb6f
customers|40
orders|320
order_details|788
```

```sh
SMUGGLR=/Volumes/store/projects/rafters-studio/smugglr/target/debug/smugglr ../westwind/make.sh --empty ./remote.db
```

```
Applied migration v1 (1 op) -- checksum f40fed16bdb497a97d42a748a217ff175950ddcd4f5d34d007e0464fc5323f5f
Applied migration v2 (1 op) -- checksum 846afb23867541c0aa5b1e51caf0422b3b6139fdd74be483053ee963ec0149a2
Applied migration v3 (2 ops) -- checksum d45e3062fb7d3ebfc9d2c1ddaf5b1bfa1f01832d8f7d5c68fa6608b8c3ed81c2
Applied migration v4 (1 op) -- checksum c6447ae278a2e42d6b1cbf73e7e262ceac42b352b9ab77e483597f229b3f5942
Applied migration v5 (1 op) -- checksum c0bef91533708c8d4e3fc5c40f80fb9c5ea5db05f67becd58a994ff814fcbc25
Applied migration v6 (1 op) -- checksum 78ef6eae8eac922753460c0261d06006446d1b951298b0c11b1b6d2171e2828e
Applied migration v7 (3 ops) -- checksum 4b5cce42abd1e797066f5bfbebf4a96a53f3e8ff62d3ab6d0ad91cc703cbd701
Applied migration v8 (2 ops) -- checksum 282301d633321a5785ba012e5a176443fdc21981cb5c690dce9f24e18809bb6f
customers|0
orders|0
order_details|0
```

The migrations also create `_smugglr_migrations` on each side. The default `[sync].exclude_tables` covers it, so the service reports eight tables, not nine.

Start the endpoint in one shell. It logs each request to stderr.

```sh
python3 serve.py remote.db 18787
```

```
serve.py: remote.db on http://127.0.0.1:18787/sql
```

Run the service in a second shell with a short interval. The first tick fires at startup and pushes the whole seed to the empty remote. About two seconds in, a third shell inserted one customer on the remote side with `sqlite3 remote.db "INSERT INTO customers VALUES ('01a04a7c-3540-722c-81ff-f2200745a95c', 'WINTR', 'Winterfell', 'Sansa Stark', 'Lady of Winterfell', 'Winterfell', 'The North', 1736240400);"`, and after the second tick the service was sent SIGINT. Both streams are shown; the `Finished` and `Running` lines are cargo's. Table order within a tick comes from a hash set and varies between runs.

```sh
SYNC_INTERVAL=3 SMUGGLR_HTTP_SQL_PLUGIN=../../../target/release/smugglr-http-sql cargo run --release
```

```
    Finished `release` profile [optimized] target(s) in 0.23s
     Running `target/release/smugglr-example-tokio-service`
2026-08-28T22:27:30.933137Z  INFO smugglr_core::local: Opening local database: local.db
2026-08-28T22:27:30.950352Z  INFO smugglr_example_tokio_service: syncing every 3s
2026-08-28T22:27:30.953201Z  INFO smugglr_core::sync: Found 8 tables to sync
2026-08-28T22:27:30.953212Z  INFO smugglr_core::diff: Computing diff for table: orders
2026-08-28T22:27:30.955695Z  INFO smugglr_core::sync: Pushing 320 rows to table: orders (dry_run=false)
2026-08-28T22:27:30.967721Z  INFO smugglr_core::sync: No changes to pull for table: orders
2026-08-28T22:27:30.967737Z  INFO smugglr_core::diff: Computing diff for table: products
2026-08-28T22:27:30.969179Z  INFO smugglr_core::sync: Pushing 20 rows to table: products (dry_run=false)
2026-08-28T22:27:30.970883Z  INFO smugglr_core::sync: No changes to pull for table: products
2026-08-28T22:27:30.970894Z  INFO smugglr_core::diff: Computing diff for table: shippers
2026-08-28T22:27:30.972506Z  INFO smugglr_core::sync: Pushing 3 rows to table: shippers (dry_run=false)
2026-08-28T22:27:30.974398Z  INFO smugglr_core::sync: No changes to pull for table: shippers
2026-08-28T22:27:30.974412Z  INFO smugglr_core::diff: Computing diff for table: categories
2026-08-28T22:27:30.975598Z  INFO smugglr_core::sync: Pushing 8 rows to table: categories (dry_run=false)
2026-08-28T22:27:30.977092Z  INFO smugglr_core::sync: No changes to pull for table: categories
2026-08-28T22:27:30.977109Z  INFO smugglr_core::diff: Computing diff for table: suppliers
2026-08-28T22:27:30.978027Z  INFO smugglr_core::sync: Pushing 8 rows to table: suppliers (dry_run=false)
2026-08-28T22:27:30.979231Z  INFO smugglr_core::sync: No changes to pull for table: suppliers
2026-08-28T22:27:30.979261Z  INFO smugglr_core::diff: Computing diff for table: customers
2026-08-28T22:27:30.980624Z  INFO smugglr_core::sync: Pushing 40 rows to table: customers (dry_run=false)
2026-08-28T22:27:30.982715Z  INFO smugglr_core::sync: No changes to pull for table: customers
2026-08-28T22:27:30.982743Z  INFO smugglr_core::diff: Computing diff for table: employees
2026-08-28T22:27:30.983904Z  INFO smugglr_core::sync: Pushing 9 rows to table: employees (dry_run=false)
2026-08-28T22:27:30.985559Z  INFO smugglr_core::sync: No changes to pull for table: employees
2026-08-28T22:27:30.985567Z  INFO smugglr_core::diff: Computing diff for table: order_details
2026-08-28T22:27:30.988526Z  INFO smugglr_core::sync: Pushing 788 rows to table: order_details (dry_run=false)
2026-08-28T22:27:31.008062Z  INFO smugglr_core::sync: No changes to pull for table: order_details
2026-08-28T22:27:31.008112Z  INFO smugglr_example_tokio_service: sync ok: 8 tables, 1196 rows pushed, 0 rows pulled
2026-08-28T22:27:33.953938Z  INFO smugglr_core::sync: Found 8 tables to sync
2026-08-28T22:27:33.953971Z  INFO smugglr_core::diff: Computing diff for table: orders
2026-08-28T22:27:33.959658Z  INFO smugglr_core::sync: Table orders is in sync
2026-08-28T22:27:33.959703Z  INFO smugglr_core::diff: Computing diff for table: customers
2026-08-28T22:27:33.961489Z  INFO smugglr_core::sync: No changes to push for table: customers
2026-08-28T22:27:33.961520Z  INFO smugglr_core::sync: Pulling 1 rows to table: customers (dry_run=false)
2026-08-28T22:27:33.964050Z  INFO smugglr_core::local: Upserted 1 rows into customers
2026-08-28T22:27:33.964065Z  INFO smugglr_core::diff: Computing diff for table: shippers
2026-08-28T22:27:33.965178Z  INFO smugglr_core::sync: Table shippers is in sync
2026-08-28T22:27:33.965207Z  INFO smugglr_core::diff: Computing diff for table: employees
2026-08-28T22:27:33.966986Z  INFO smugglr_core::sync: Table employees is in sync
2026-08-28T22:27:33.967006Z  INFO smugglr_core::diff: Computing diff for table: categories
2026-08-28T22:27:33.968295Z  INFO smugglr_core::sync: Table categories is in sync
2026-08-28T22:27:33.968304Z  INFO smugglr_core::diff: Computing diff for table: order_details
2026-08-28T22:27:33.974988Z  INFO smugglr_core::sync: Table order_details is in sync
2026-08-28T22:27:33.975049Z  INFO smugglr_core::diff: Computing diff for table: suppliers
2026-08-28T22:27:33.976083Z  INFO smugglr_core::sync: Table suppliers is in sync
2026-08-28T22:27:33.976093Z  INFO smugglr_core::diff: Computing diff for table: products
2026-08-28T22:27:33.977720Z  INFO smugglr_core::sync: Table products is in sync
2026-08-28T22:27:33.977737Z  INFO smugglr_example_tokio_service: sync ok: 8 tables, 0 rows pushed, 1 rows pulled
2026-08-28T22:27:35.069207Z  INFO smugglr_example_tokio_service: shutdown signal received
```

The endpoint's shell logged its startup line and then 46 lines of `serve.py: "POST /sql HTTP/1.1" 200 -` over the same run: one `SELECT 1` on plugin start, then the table list, per-table `PRAGMA table_info` (cached by the plugin after the first tick), metadata selects, batch writes, and the row fetch for the pulled customer. Afterwards both files hold the same rows, including the customer that only ever existed on the remote.

```sh
sqlite3 local.db "SELECT 'customers', count(*) FROM customers UNION ALL SELECT 'orders', count(*) FROM orders UNION ALL SELECT 'order_details', count(*) FROM order_details;"
```

```
customers|41
orders|320
order_details|788
```

```sh
sqlite3 remote.db "SELECT 'customers', count(*) FROM customers UNION ALL SELECT 'orders', count(*) FROM orders UNION ALL SELECT 'order_details', count(*) FROM order_details;"
```

```
customers|41
orders|320
order_details|788
```

```sh
sqlite3 local.db "SELECT code, company, contact FROM customers WHERE code = 'WINTR';"
```

```
WINTR|Winterfell|Sansa Stark
```

## What this demonstrates

Driving the sync engine, `smugglr_core::sync::sync_all`, without the CLI's orchestration layer. You construct the two `DataSource` values and the `Config` yourself; `sync_all` takes a table list (`None` discovers the tables both sides share), a dry-run flag, and a `SyncProgress` reporter (`NoProgress` here). Conflict resolution comes from `[sync].conflict_resolution` in the config string; the example sets `newer_wins`.

The two `DataSource` implementations are the ones the CLI uses, wired by hand. `smugglr_core::local::LocalDb` wraps a rusqlite connection. `smugglr_core::plugin::PluginDataSource` spawns the `smugglr-http-sql` binary and speaks JSON-RPC to it over stdin and stdout; the binary posts SQL to the endpoint with the profile named in its config. There is no HTTP client in `smugglr-core`'s own `DataSource` set; every remote backend goes through that plugin.

Shutdown is two-stage, and the ordering is deliberate. The `tokio::select!` waits on the tick and on `tokio::signal::ctrl_c()` while the service is idle, with `biased;` so the signal is polled first and its handler is installed on the first pass. The sync itself runs after the `select!`, outside it, so a signal cannot cancel a sync midway; a SIGINT that arrives during a sync is seen at the next `select!`, after that sync returns, and the loop exits. Dropping the `PluginDataSource` kills the plugin process.

One caveat on a terminal `Ctrl-C`. The plugin is a child in the same foreground process group, so the terminal delivers SIGINT to it as well, and it exits. Between ticks that changes nothing; a SIGINT sent to the whole group between ticks produced the same `shutdown signal received` line and a clean exit. If the plugin dies while a sync is mid-call, that sync returns an error, the service logs `sync failed`, and then shuts down. A supervisor that signals the service pid alone, as `kill -INT` did for the run above, leaves the plugin alive for the in-flight sync to finish.
