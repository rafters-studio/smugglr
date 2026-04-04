<p align="center">
  <img src="smugglr.webp" alt="smugglr" width="400">
</p>

# smugglr

SQLite replication across platforms. Local files, Turso, Cloudflare D1, rqlite, SQLite Cloud, StarbaseDB, Datasette -- same diff engine, same content hashing, any target.

## Status: Beta

Running in production since early 2026. Plugin architecture with 7 HTTP SQL targets shipping out of the box. 219 tests, CI across Linux, macOS, and Windows.

Not 1.0 yet. The API surface may still shift. But the sync engine is solid: content-hash diffing, delta sync, bidirectional replication, LAN broadcast, S3 relay, and agent-friendly JSON output.

## What It Does

smugglr replicates SQLite data between platforms. It compares rows by content hash, computes deltas, and moves only what changed.

- **Content hashing** -- SHA256 of actual row data, not timestamps
- **Delta sync** -- only moves rows that changed
- **Bidirectional** -- push, pull, or both with configurable conflict resolution
- **Plugin architecture** -- `DataSource` trait for built-in backends, JSON-RPC plugin protocol for external adapters
- **7 HTTP SQL targets** -- Turso, rqlite, D1, Datasette, SQLite Cloud, StarbaseDB, generic HTTP SQL
- **LAN broadcast** -- UDP peer discovery + XChaCha20-Poly1305 encrypted delta exchange
- **S3 relay** -- cross-machine sync through any S3-compatible store (R2, MinIO, GCS)
- **Watch daemon** -- background sync on a configurable interval
- **Agent-friendly** -- `--output json` on all commands, `--dry-run` returns structured data
- **Column exclusion** -- skip embeddings and large BLOBs with glob patterns
- **No state files** -- fresh comparison every run

## Installation

### Quick install

```bash
curl -fsSL https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.sh | bash
```

### From source

```bash
cargo install --git https://github.com/rafters-studio/smugglr
```

Requires Rust 1.75+.

## Quick Start

### Direct (local SQLite to SQLite)

```toml
# config.toml
local_db = "app.db"

[target]
type = "sqlite"
database = "replica.db"
```

```bash
smugglr diff     # what's different?
smugglr push     # local -> target
smugglr pull     # target -> local
smugglr sync     # bidirectional
```

### Plugin (local SQLite to Turso)

```toml
# config.toml
local_db = "app.db"

[target]
type = "plugin"
name = "http-sql"
config = { url = "https://your-db.turso.io", auth_token = "your-token", profile = "turso" }
```

```bash
smugglr push     # local -> Turso
```

### Plugin profiles

The `smugglr-http-sql` plugin ships with profiles for:

| Profile | Target |
|---------|--------|
| `turso` | Turso / libSQL |
| `rqlite` | rqlite |
| `d1` | Cloudflare D1 REST API |
| `datasette` | Datasette |
| `sqlite-cloud` | SQLite Cloud |
| `starbasedb` | StarbaseDB |
| `generic` | Any HTTP SQL endpoint |

## Commands

```
smugglr status      # connection check
smugglr diff        # show what's different
smugglr push        # local -> target
smugglr pull        # target -> local
smugglr sync        # bidirectional
smugglr stash       # local -> S3 relay
smugglr retrieve    # S3 relay -> local
smugglr watch       # daemon mode
smugglr broadcast   # LAN peer sync
```

### Options

```
-c, --config <FILE>     Config file [default: config.toml]
-v, --verbose           Verbose output
-o, --output <FORMAT>   Output format: text or json [default: text]
--dry-run               Show what would change without applying
--table <NAME>          Sync one table only
```

## How It Works

For each table, smugglr:

1. Gets all primary keys from both sides
2. SHA256 hashes each row's content (excluding timestamp columns)
3. Compares timestamps when content differs
4. Sorts rows into buckets: `local_only`, `remote_only`, `local_newer`, `remote_newer`, `content_differs`, `identical`
5. Batches writes to respect target limits
6. Retries transient failures with exponential backoff

## Architecture

```
smugglr (CLI binary)
  |
  v
smugglr-core (library crate)
  |-- DataSource trait
  |     |-- LocalDb (rusqlite)
  |     |-- D1Client (Cloudflare REST API)
  |     |-- PluginDataSource (JSON-RPC over stdin/stdout)
  |
  |-- Diff engine (content hash, delta computation)
  |-- Sync engine (push, pull, bidirectional)
  |-- Broadcast (UDP discovery, TCP delta exchange, XChaCha20-Poly1305)
  |-- Stash/Retrieve (S3-compatible relay)

smugglr-plugin-sdk (plugin authoring crate)
  |-- PluginAdapter trait
  |-- JSON-RPC server loop

smugglr-http-sql (adapter plugin)
  |-- 7 target profiles
  |-- Configurable request/response formats
```

### Writing plugins

Implement `PluginAdapter` from `smugglr-plugin-sdk` and call `run()`:

```rust
use smugglr_plugin_sdk::{PluginAdapter, run};

struct MyAdapter;

impl PluginAdapter for MyAdapter {
    // implement list_tables, table_info, get_rows, upsert_rows, etc.
}

#[tokio::main]
async fn main() {
    run(MyAdapter).await;
}
```

Install the binary as `smugglr-<name>` in `~/.smugglr/plugins/` or on `$PATH`.

## Cross-Machine Sync

### S3 Relay (stash/retrieve)

```
Machine A  --stash-->  S3/R2  --retrieve-->  Machine B
```

```toml
[stash]
url = "s3://my-bucket/smugglr/relay.sqlite"
access_key_id = "AKIA..."
secret_access_key = "..."
endpoint = "https://account-id.r2.cloudflarestorage.com"
```

### LAN Broadcast

```
Machine A  --broadcast-->  [encrypted UDP/TCP]  <--broadcast--  Machine B
```

```toml
[broadcast]
port = 31337
interval_secs = 30
secret = "your-256-bit-hex-key"  # openssl rand -hex 32
```

All broadcast traffic is encrypted with XChaCha20-Poly1305. Designed for trusted networks. For untrusted networks, run inside a WireGuard tunnel.

## Configuration

See [config.example.toml](config.example.toml) for all options.

## Development

```bash
cargo test                       # 219 tests
cargo fmt                        # format
cargo clippy --all-targets       # lint
```

## License

MIT.

---

Part of [rafters-studio](https://github.com/rafters-studio).
