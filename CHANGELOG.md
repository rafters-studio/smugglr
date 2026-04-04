# Changelog

## 0.2.1 (2026-04-02)

### Added

- **`--output json` flag** across all commands for machine-parseable output. Agents and scripts get structured JSON instead of human-readable text. (#32)
- **Structured exit codes** for error classification: config errors (10), network transient (20), conflict (30), auth (40). Callers can distinguish "retry later" from "fix config" from "human intervention needed." (#33)
- **LAN broadcast sync** (`smuggler broadcast`): peer discovery via UDP subnet broadcast on port 31337, delta serialization wire protocol with automatic packet splitting, TCP sync exchange between peers. Designed for keeping databases consistent across machines on the same network. (#35, #38, #39)
- **XChaCha20-Poly1305 encryption** for all broadcast traffic. Pre-shared key, known-network threat model. Every packet on the wire is authenticated and encrypted. (#45)
- **Column-level exclusion** (`exclude_columns` in config): glob patterns like `*_embedding` strip columns from sync. Useful for skipping large BLOB columns (embeddings, vectors) that don't need to sync. (#36)
- **UUIDv7 conflict resolution** (`conflict_resolution = "uuid_v7_wins"`): requires UUIDv7 primary keys for master-master sync. Prevents insert collisions across machines. Hard error if PKs are not UUIDv7 -- no silent data corruption. (#37)
- **Watch daemon** (`smuggler watch`): background sync on a configurable interval with PID locking to prevent duplicate daemons. (#26)
- **Local SQLite target** (`[target] type = "sqlite"`): sync between two SQLite databases without D1. (#27)
- **Bidirectional sync command** (`smuggler sync`): push + pull in one operation. (#25)
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
