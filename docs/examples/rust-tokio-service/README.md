# rust-tokio-service

Embed smugglr inside a long-running tokio service. The service holds a local SQLite database open, calls `sync_all` on a tick, and shuts down cleanly. Bypasses the CLI entirely -- this is the right pattern for backends that already own their database connection.

## Prerequisites

- Rust stable
- A local SQLite file at the path in `LOCAL_DB`
- A reachable HTTP-SQL target. The example points at a generic profile so any backend that speaks `{sql, params}` -> `{columns, rows}` works (rqlite, your own gateway, etc.). Replace with `Profile::turso()` / `Profile::d1()` for the hosted services.

## Run

```sh
cargo run --release
```

The service syncs on startup, then every `SYNC_INTERVAL` (default 30s). `Ctrl-C` triggers a graceful shutdown that completes the in-flight sync before exit.

## What this demonstrates

- Driving smugglr's sync engine (`smugglr_core::sync::sync_all`) directly without the CLI orchestration layer. You bring your own `DataSource` instances and conflict resolution.
- Using `tokio::select!` to interleave the sync tick with shutdown signals.
- The two `DataSource` impls used here -- `LocalSqlite` (rusqlite) and the generic HTTP profile -- are the same impls the CLI uses, just wired by hand.
