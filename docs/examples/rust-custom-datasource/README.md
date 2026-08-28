# rust-custom-datasource

Implement the `DataSource` trait against a non-standard store. The example wraps a `HashMap` of rows in memory; the same shape works for a Redis hash, an object-store JSON blob, a custom HTTP API, or any place row-shaped data lives.

This example is also where to look if you want to understand smugglr's content-hashed delta model from the inside.

## Prerequisites

Rust stable. The example depends on `smugglr-core` 0.5.0 by path into this repository's `crates/smugglr-core`; outside the repository, drop the `path` key from `Cargo.toml` and the same version resolves from crates.io. Nothing here touches the network.

## Run

```sh
cargo run --release
```

```
before: a=2, b=2
  a: w1 alpha @ 2026-04-25T00:00:00Z
  a: w3 gamma @ 2026-04-25T00:00:00Z
  b: w2 beta @ 2026-04-25T00:00:01Z
  b: w3 gamma-edited @ 2026-04-25T00:00:05Z
sync:   widgets pushed a->b=1, pulled b->a=2
after:  a=3, b=3
  a: w1 alpha @ 2026-04-25T00:00:00Z
  a: w2 beta @ 2026-04-25T00:00:01Z
  a: w3 gamma-edited @ 2026-04-25T00:00:05Z
  b: w1 alpha @ 2026-04-25T00:00:00Z
  b: w2 beta @ 2026-04-25T00:00:01Z
  b: w3 gamma-edited @ 2026-04-25T00:00:05Z
```

Two stores start divergent: `w1` exists only on `a`, `w2` only on `b`, and `w3` on both with `b` holding the newer edit. One `sync_all` call pushes `w1` to `b`, pulls `w2` to `a`, and resolves `w3` by `updated_at` because the config sets `conflict_resolution = "newer_wins"`. The default, `local_wins`, would keep `a`'s copy. `rows_pushed` counts rows written from the first argument to the second; `rows_pulled` counts the reverse.

## What this demonstrates

The whole `DataSource` surface, as declared in `smugglr_core::datasource`:

| Method | Called for |
| --- | --- |
| `list_tables` | table discovery when `sync_all` gets `None` for the table list |
| `table_info` | column list and primary key; during discovery a table with no primary key is skipped |
| `get_row_metadata` | the diff: one `RowMeta` per row, keyed by primary key |
| `get_rows` | row bodies, only for primary keys the diff decided to transfer |
| `upsert_rows` | writing transferred rows to the destination |
| `row_count` | reporting |

`RowMeta::content_hash` is what smugglr compares. `diff_table` calls `get_row_metadata` on both sides and classifies every primary key from the two metadata maps alone; `get_rows` runs afterwards, for the keys that need to move. As long as `get_row_metadata` returns a stable hash for an unchanged row, the row body is never requested. Both sides here share one hashing routine; two different backends must hash the same bytes the same way, or the same row reads as changed forever.

This is why content hashing beats log-driven change capture for cross-store sync. There is no log to subscribe to, no replication slot to manage, and no requirement that both sides understand each other's transaction model. You sync the state, not the operations that produced it.

`upsert_rows` merges into the existing row rather than replacing it. The trait documents that a column absent from an incoming row must be left alone: an existing row keeps its stored value, and a new row takes the schema default. `[sync].exclude_columns` strips matching columns before transfer, so an implementation that wrote NULL for an absent column would destroy the value the operator configured to stay off the wire.

The trait's futures carry a `MaybeSend` bound, defined in `smugglr_core::datasource`. On native targets it is `Send`; on `wasm32` it is empty. An `async fn` implementation that holds no non-`Send` value across an `.await` satisfies both without changes, which is why this file has no `cfg` in it. The methods here contain no `.await` at all, so the `std::sync::MutexGuard` they hold is never a problem.
