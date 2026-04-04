<p align="center">
  <img src="smugglr.webp" alt="Smugglr" width="400">
</p>

# Smuggler

> "Look, I ain't in this for your revolution, and I'm not in it for you, Princess. I expect to be well paid. I'm in it for the money." - Han Solo

Smuggle data between SQLite databases, Cloudflare D1, and S3-compatible stores. Fast. Stateless. Encrypted. Questionable life choices.

## Status: Beta (Kessel Run Certified)

Running in production at [huttspawn.com](https://huttspawn.com) since early 2026. Pluggable data source architecture. CI across Linux, macOS, and Windows. Checksummed releases with a one-line installer.

Not 1.0 yet -- the API surface may still shift. But the core sync engine is solid and battle-tested, with LAN broadcast sync, S3 relay, and agent-friendly JSON output.

There are [open issues](https://github.com/rafters-studio/smugglr/issues). We're shaving parsecs, not days.

## What It Does

Smuggler is a universal SQLite sync engine. It started as a way to sync local dev databases with Cloudflare D1, but now handles SQLite-to-SQLite, cross-machine LAN sync, and S3 relay workflows.

- **Content hashing** - SHA256 comparison of actual row data, not just timestamps
- **Delta sync** - Only moves rows that changed
- **Bidirectional** - Push, pull, or both with configurable conflict resolution
- **LAN broadcast sync** - UDP peer discovery + encrypted delta exchange between machines on the same subnet
- **Agent-friendly** - `--output json` across all commands, structured exit codes for programmatic callers
- **No state files** - Fresh comparison every run, no stale state to haunt you
- **Pluggable backends** - `DataSource` trait abstracts any database backend
- **Watch daemon** - Background sync on a configurable interval
- **Column exclusion** - Skip embedding BLOBs and other large columns with glob patterns
- **Automatic retries** - Exponential backoff with configurable limits for transient D1 failures
- **Batched writes** - Respects D1's 100-parameter bind limit, splits large upserts automatically
- **Table validation** - `--table` input validated against live schema before any SQL runs

## Installation

### Quick install (recommended)

```bash
curl -fsSL https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.sh | bash
```

Detects your platform, downloads the right binary, verifies the SHA256 checksum, and installs to `~/.local/bin/`. Supports Linux x64, macOS x64, and macOS ARM64. Detects Rosetta 2 and installs the native arm64 binary.

Install a specific version:

```bash
curl -fsSL https://raw.githubusercontent.com/rafters-studio/smugglr/main/install.sh | bash -s v0.1.2
```

### Manual download

| Platform | Download |
|----------|----------|
| Linux x64 | [smugglr-linux-x64.tar.gz](https://github.com/rafters-studio/smugglr/releases/latest/download/smugglr-linux-x64.tar.gz) |
| macOS x64 | [smugglr-macos-x64.tar.gz](https://github.com/rafters-studio/smugglr/releases/latest/download/smugglr-macos-x64.tar.gz) |
| macOS ARM64 | [smugglr-macos-arm64.tar.gz](https://github.com/rafters-studio/smugglr/releases/latest/download/smugglr-macos-arm64.tar.gz) |
| Windows x64 | [smugglr-windows-x64.zip](https://github.com/rafters-studio/smugglr/releases/latest/download/smugglr-windows-x64.zip) |

### From source

```bash
cargo install --git https://github.com/rafters-studio/smugglr
```

Requires Rust 1.75+.

## Quick Start

1. Copy the example config:

```bash
cp config.example.toml config.toml
```

2. Add your credentials (don't commit this file, genius):

```toml
cloudflare_account_id = "your-account-id"
cloudflare_api_token = "your-api-token"
database_id = "your-d1-database-id"
local_db = ".wrangler/state/v3/d1/miniflare-D1DatabaseObject/xxx.sqlite"
```

3. Check if you can reach D1:

```bash
smugglr status
```

4. See what's different:

```bash
smugglr diff
```

5. Push your local changes (point of no return):

```bash
smugglr push
```

## Commands

```
smugglr status      # Can we phone home?
smugglr diff        # What's different?
smugglr push        # Local -> D1 (YOLO)
smugglr pull        # D1 -> Local (safer YOLO)
smugglr sync        # Bidirectional (push + pull in one shot)
smugglr stash       # Local -> S3 relay (cross-machine sync)
smugglr retrieve    # S3 relay -> Local (cross-machine sync)
smugglr watch       # Daemon mode (sync on interval)
smugglr broadcast   # LAN sync (peer discovery + encrypted deltas)
```

### Options

```
-c, --config <FILE>     Config file [default: config.toml]
-v, --verbose           See what's happening under the hood
-o, --output <FORMAT>   Output format: text or json [default: text]
--dry-run               Coward mode (just kidding, it's smart)
--table <NAME>          Sync one table only (validated against schema)
```

## How It Works

For each table, Smuggler:

1. Grabs all primary keys from both databases
2. SHA256 hashes each row's content (excluding timestamp columns)
3. When content differs, compares timestamps to determine which side is newer
4. Sorts rows into buckets:

| Bucket | What it means | Push | Pull |
|--------|--------------|------|------|
| `local_only` | You added it locally | Insert to D1 | - |
| `remote_only` | Someone else added it | - | Insert locally |
| `local_newer` | Your timestamp wins | Update D1 | - |
| `remote_newer` | Their timestamp wins | - | Update local |
| `content_differs` | Same timestamp, different data | Configurable | Configurable |
| `identical` | Exactly the same | Skip | Skip |

5. Batches writes to stay within D1's bind parameter limits (100 params per statement)
6. Retries transient failures with exponential backoff

### Why content hashing?

Timestamps lie. Clocks drift. Bulk imports set everything to "now". Content hashing catches actual changes regardless of what the timestamps say.

### Tables without timestamp columns

Smuggler gracefully handles tables missing the configured timestamp column. Rows with different content land in the `content_differs` bucket and are resolved by your `conflict_resolution` setting. Use `local_wins` or `remote_wins` for deterministic behavior on these tables -- `newer_wins` will skip them since there's no timestamp to compare.

## Architecture

Smuggler's sync engine is built on the `DataSource` trait, which abstracts any database backend:

```
DataSource (trait)
  |-- LocalDb    (rusqlite, synchronous)
  |-- D1Client   (reqwest HTTP, async with retries)
```

The diff engine (`diff_table`) and table resolution (`get_tables_to_sync`) are generic over any two `DataSource` implementations. This means the same comparison logic works whether you're syncing local-to-D1, local-to-local, or any future backend.

## Cross-Machine Sync (Stash/Retrieve)

Smuggler can sync your local SQLite database through an S3-compatible object store, enabling multiple dev machines to share state without going through D1.

```
Machine A                    S3/R2/GCS                  Machine B
  local.sqlite  --stash-->  relay.sqlite  --retrieve-->  local.sqlite
                <--retrieve--             <--stash--
```

### How it works

Both sides are SQLite. Smuggler downloads the relay file from S3, opens it as a second `LocalDb`, and runs the same diff engine used for D1 sync. Only changed rows are transferred.

- **`smugglr stash`** -- diffs local against the relay, applies changes to the relay, uploads it back to S3
- **`smugglr retrieve`** -- downloads the relay from S3, diffs it against local, applies changes to local

On first stash, Smuggler creates the relay from scratch and initializes its schema from the local database. ETag conditional writes prevent concurrent overwrites when multiple machines stash at the same time.

### Stash configuration

Add a `[stash]` section to your config (independent from D1 settings):

```toml
[stash]
# S3-compatible URL to the relay file
url = "s3://my-bucket/smugglr/relay.sqlite"

# AWS credentials (optional if using instance roles or env vars)
access_key_id = "AKIA..."
secret_access_key = "..."
region = "us-east-1"

# Custom endpoint for Cloudflare R2, MinIO, etc.
endpoint = "https://account-id.r2.cloudflarestorage.com"
```

Supported URL schemes:
- `s3://bucket/path` -- Amazon S3 or any S3-compatible store (R2, MinIO)
- `file:///absolute/path` -- Local filesystem (useful for testing)

### Usage with automation

Stash/retrieve pairs well with session hooks. For example, with [Legion](https://github.com/ssilvius/legion):

```bash
# SessionStart hook
smugglr retrieve && legion reindex

# SessionStop hook
smugglr stash
```

## LAN Broadcast Sync

Smuggler can keep SQLite databases in sync across machines on the same LAN. No relay server, no cloud dependency -- just encrypted UDP broadcast for peer discovery and TCP for delta exchange.

```
Machine A                  LAN (port 31337)               Machine B
  legion.db  --broadcast-->  [encrypted deltas]  <--broadcast--  legion.db
```

### How it works

1. `smugglr broadcast` sends UDP announcements on port 31337
2. Peers on the same subnet discover each other automatically
3. On discovery, peers exchange deltas over TCP (only changed rows)
4. All traffic is encrypted with XChaCha20-Poly1305 (pre-shared key)
5. UUIDv7 primary keys prevent insert collisions across machines

### Broadcast configuration

```toml
[broadcast]
port = 31337
interval_secs = 30

# 256-bit key, hex-encoded. Generate with: openssl rand -hex 32
# ALL broadcast traffic is encrypted when this is set.
secret = "your-256-bit-hex-key"

[sync]
# Skip large columns (embeddings, vectors) from broadcast
exclude_columns = ["*_embedding", "vector"]

# UUIDv7 required for master-master sync
conflict_resolution = "uuid_v7_wins"
```

### Security model

Designed for **known/trusted networks** (home LAN, office LAN). The pre-shared key prevents eavesdropping and tampering on the local subnet. This is not designed for hostile networks or the open internet. If you need that, run smugglr inside a WireGuard tunnel.

## Configuration

```toml
cloudflare_account_id = "abc123"
cloudflare_api_token = "your-token-with-d1-permissions"
database_id = "your-d1-uuid"
local_db = "/path/to/local.sqlite"

[sync]
# Empty = sync all tables except excluded
tables = []

# Things you definitely don't want to sync
exclude_tables = [
    "sqlite_sequence",
    "_cf_KV",
    "__drizzle_migrations",
]

# Column for timestamp ordering when content differs
timestamp_column = "updated_at"

# When both sides changed: "local_wins", "remote_wins", "newer_wins", "uuid_v7_wins"
conflict_resolution = "local_wins"

# Columns to exclude from sync (glob patterns)
exclude_columns = ["*_embedding", "vector"]

# Retry settings for transient D1 failures
max_retries = 5
initial_retry_delay_ms = 100
max_retry_delay_ms = 30000
backoff_multiplier = 2.0

# Batch settings for large tables
batch_size = 100
max_statement_bytes = 92160
```

### Finding Your Local D1 Database

Wrangler hides it at:

```
.wrangler/state/v3/d1/miniflare-D1DatabaseObject/<hash>.sqlite
```

The hash is derived from your binding name. If you have multiple databases, may the Force be with you.

### API Token

Get one from [Cloudflare Dashboard](https://dash.cloudflare.com/profile/api-tokens):

- D1:Read - for `diff`, `pull`, `status`
- D1:Write - for `push`

Pro tip: Create one token with both permissions. Fewer tokens to lose.

## Limitations

Things we don't do (yet):

- **Schema sync** - Run your migrations separately, we're data movers not DDL runners
- **Full-sync transactions** - Each batch is atomic, but the whole sync isn't. Re-run if interrupted.
- **BLOB wizardry** - Binary data compared as hex strings. It works but it's not pretty.
- **Tables without primary keys** - We need something to compare. Add a PK.
- **Linux ARM64** - Not available yet. See [issues](https://github.com/rafters-studio/smugglr/issues) for updates.

## Troubleshooting

### "401 Unauthorized"
Your token is expired or wrong. Make a new one.

### "429 Too Many Requests"
Smuggler retries automatically with exponential backoff. If you're still hitting rate limits, increase `initial_retry_delay_ms` or reduce `batch_size` in your config.

### "Invalid table name"
Smuggler validates `--table` input against your database schema before touching any SQL. If the table doesn't exist, you'll get a list of available tables. Run your migrations on both databases first. We don't create tables.

### All rows show as "content_differs"
Check that column order and types match. NULL vs empty string will cause hash mismatches.

## Development

```bash
cargo test                       # Run the tests (165 and counting)
cargo fmt                        # Format code
cargo clippy --all-targets       # Lint (including tests)
RUST_LOG=debug cargo run -- diff # Debug output
```

## Related Projects

Part of the [rafters-studio](https://github.com/rafters-studio) ecosystem. Built for [huttspawn.com](https://huttspawn.com) and the broader rafters portfolio.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). TL;DR:

1. Fork it
2. Branch it
3. Fix it / Build it
4. Test it
5. PR it

## License

MIT. Do whatever you want. Not our fault if it deletes your production database.

---

*"Never tell me the odds."*
