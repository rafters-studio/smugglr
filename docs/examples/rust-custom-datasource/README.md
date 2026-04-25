# rust-custom-datasource

Implement the `DataSource` trait against a non-standard store. The example wraps a `HashMap` of rows in memory; the same shape works for a Redis hash, an object-store JSON blob, a custom HTTP API, or any place row-shaped data lives.

This example is also where to look if you want to understand smugglr's content-hashed delta model from the inside.

## Prerequisites

- Rust stable

## Run

```sh
cargo run --release
```

You'll see the in-memory store on each side diverge, then sync, then converge.

## What this demonstrates

- The full `DataSource` surface area: `list_tables`, `table_info`, `get_row_metadata`, `get_rows`, `upsert_rows`, `row_count`.
- `RowMeta::content_hash` is what smugglr compares to decide which rows differ. As long as your `get_row_metadata` returns a stable hash for unchanged rows, the diff engine will not request the row body unless it actually needs to transfer it.
- Why content hashing beats log-driven CDC for cross-store sync: there's no log to subscribe to, no replication slot to manage, and no requirement that both sides understand each other's transaction model. You sync the *state*, not the *operations* that produced it.
- The `MaybeSend` relaxation on the trait: native targets require `Send`, WASM targets relax it. The same impl can compile for both by using the `MaybeSend` alias from `smugglr_core::datasource`.
