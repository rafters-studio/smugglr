# Changelog

## 0.3.0 (2026-04-11)

The core engine no longer knows about any specific remote backend. `D1Client` and `ResolvedTarget::D1` are gone from `smugglr-core`; every remote is a plugin. The same release ships the sync engine to the browser via WebAssembly and npm.

### Added

- **Durable Objects HTTP bridge template** (`templates/do-bridge/`): Cloudflare Worker that exposes a Durable Object's SQLite storage as a D1-compatible HTTP endpoint, reachable via the http-sql plugin. (#79)
- **Point-in-time snapshots**: `smugglr snapshot` / `smugglr snapshots` / `smugglr restore <timestamp>` for disaster recovery using existing stash storage. Snapshots land at `<stash-path>/snapshots/<timestamp>.sqlite`. (#78)
- **Batch upsert support in the http-sql plugin**: bulk row writes over a single HTTP SQL call respecting per-backend parameter limits. (#84)
- **`smugglr-wasm` crate**: compiles the http-sql client path to WebAssembly so browsers can run delta sync against remote SQL endpoints directly. (#86)
- **`smugglr` npm package** (initial): TypeScript wrapper over wasm-bindgen output. Exports `Smugglr.init(config)`, `.push()`, `.pull()`, `.sync()`, `.diff()`, and explicit `.dispose()`. Package subpath export `./wasm` for consumers who control WASM loading directly. (#88)
- **Incremental diff with per-table hash cache for WASM**: subsequent syncs only rehash tables whose source data has actually changed. (#96)

### Changed

- **D1 config routed through http-sql plugin internally**: `[target] type = "d1"` still works as a user-facing config shape; the runtime now synthesizes a plugin profile and launches the http-sql plugin to carry the traffic. No user-visible change. (#92)
- **`native` feature gate on platform-specific deps**: tokio, reqwest, and rusqlite are gated behind the `native` feature in `smugglr-core`, allowing the diff/sync engine to build for wasm32 without incompatible dependencies. (#77)
- **`smugglr` npm package cleanup**: fixed conditional exports ordering, removed redundant re-exports and dead initialization branches. (#98)
- **Broadcast TCP encryption spec** (`docs/plans/broadcast-tcp-encryption.md`): design document for encryption and TCP framing for cross-process broadcast sync. Spec only; no implementation yet. (#95)

### Removed

- **`D1Client` and `ResolvedTarget::D1` removed from `smugglr-core`**: `crates/smugglr-core/src/remote.rs` deleted (904 lines). The sync engine has no hardcoded knowledge of any remote backend. D1, Turso, rqlite, and every other HTTP SQL target are plugin concerns. (#99)

### Fixed

- Gate `smugglr-wasm` crate on `target_arch = "wasm32"` so `cargo test --workspace` on native does not attempt to compile WASM-only code. (#94)

## 0.2.1 (2026-04-02)

### Added

- **`--output json` flag** across all commands for machine-parseable output. Agents and scripts get structured JSON instead of human-readable text. (#32)
- **Structured exit codes** for error classification: config errors (10), network transient (20), conflict (30), auth (40). Callers can distinguish "retry later" from "fix config" from "human intervention needed." (#33)
- **LAN broadcast sync** (`smugglr broadcast`): peer discovery via UDP subnet broadcast on port 31337, delta serialization wire protocol with automatic packet splitting, TCP sync exchange between peers. Designed for keeping databases consistent across machines on the same network. (#35, #38, #39)
- **XChaCha20-Poly1305 encryption** for all broadcast traffic. Pre-shared key, known-network threat model. Every packet on the wire is authenticated and encrypted. (#45)
- **Column-level exclusion** (`exclude_columns` in config): glob patterns like `*_embedding` strip columns from sync. Useful for skipping large BLOB columns (embeddings, vectors) that don't need to sync. (#36)
- **UUIDv7 conflict resolution** (`conflict_resolution = "uuid_v7_wins"`): requires UUIDv7 primary keys for master-master sync. Prevents insert collisions across machines. Hard error if PKs are not UUIDv7 -- no silent data corruption. (#37)
- **Watch daemon** (`smugglr watch`): background sync on a configurable interval with PID locking to prevent duplicate daemons. (#26)
- **Local SQLite target** (`[target] type = "sqlite"`): sync between two SQLite databases without D1. (#27)
- **Bidirectional sync command** (`smugglr sync`): push + pull in one operation. (#25)
- **Generic DataSource sync**: the sync engine is fully generic over the `DataSource` trait, enabling local-to-local, local-to-D1, or any future backend pair. (#24)

### Changed

- Migrated org references from ezmode-games to rafters-studio. (#28)

## 0.2.0 (2026-03-16)

- S3-compatible relay sync (`stash`/`retrieve`)
- DataSource trait extraction
- Batch operations with D1 parameter limit awareness
- Table name validation against live schema
- Automatic retry with exponential backoff

## 0.1.2 (2026-03-09)

- Initial public beta
- Push, pull, diff, status commands
- Content-hash change detection
- Configurable conflict resolution
