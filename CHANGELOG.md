# Changelog

## 0.4.2 (2026-07-14)

Release-versioning alignment. The `@smugglr/zustand` and `@smugglr/nanostores` bridge packages had drifted to an independent `0.1.0` and are now versioned in lockstep with the rest of the release, so every published artifact -- the crates, the `smugglr` npm package, and the two bridge packages -- shares one version. No functional change from 0.4.1.

## 0.4.1 (2026-07-13)

A correctness release for the sync engine. Six bugs in the change-detection and conflict-resolution path -- most of them silent -- are fixed: integer Unix timestamps now sync and order correctly, the plugin and wasm adapters gain the NULL-/duplicate-primary-key guards core already had, the datasette profile stops dropping bind parameters, `get_rows` guards the empty-primary-key case, non-UTF-8 text no longer hashes as NULL, and snapshot object keys are writable on Windows.

### Fixed

- **Integer Unix timestamps sync and order correctly.** The remote adapters (http-sql plugin, wasm fetch) read `updated_at` via `as_str()` only, so a JSON integer timestamp came back `None` while the local side had `Some(...)` -- every content-changed row fell into `content_differs` and was silently skipped in both directions under `newer_wins`/`uuid_v7_wins`. Separately, `classify_diff` compared timestamps as raw strings, reversing the conflict direction across a digit-count boundary (`"999"` sorts after `"1000"`). A single canonical `extract_updated_at` now renders integer timestamps for all three paths, and comparison is numeric-aware, routing mixed representations to `content_differs` rather than guessing. (#177, #176)
- **NULL-/duplicate-primary-key guard parity in the plugin and wasm metadata builders.** A NULL rendered `__pk` was coerced to `""`, collapsing every such row onto one map key (silent drops, spurious deletes), and a duplicate PK-text overwrote its entry silently. Both paths now skip a NULL `__pk` with a warning and surface a duplicate, matching core `local.rs`. (#231)
- **The datasette profile rejects parameterized queries instead of dropping them.** `build_request` discarded the params slice for the datasette profile, sending `?` placeholders with zero bound values (endpoint error or mis-bind). It now returns a clear error -- Datasette has no positional-bind API. (#201)
- **`get_rows` guards the empty-primary-key case.** An empty primary key rendered to malformed `WHERE  IN (?, ?)` SQL; all three adapters now build the query through one guarded helper that errors instead. (#198)
- **Non-UTF-8 text no longer hashes as NULL.** `get_json_value` swallowed each typed read's error and returned NULL, folding a non-UTF-8 text column into the content hash indistinguishably from a real NULL (a stable-but-wrong hash). It now inspects the value's storage class once and errors on the undecodable case. (#180)
- **Snapshot object keys are filename-safe on Windows.** Keys embedded a colon-bearing timestamp (`HH:MM:SS`), invalid in a Windows filename; they are now colon-free, with restore falling back to the legacy colon key so snapshots written by an earlier version still restore. (#238)

## 0.4.0 (2026-05-30)

Browser sync surface fills in. The npm package gets the four runtime affordances a real app needs (auto-sync, anonymous-first, auth rotation, right-to-erasure), the OPFS local-source path lands behind `wa-sqlite`, and a `table-changed` event lets reactive plugins react without polling. Two such plugins ship: `@smugglr/zustand` and `@smugglr/nanostores`. On the config side, `config.toml` gains `${VAR}` secret expansion and the documented retry/backoff now actually runs on the write path. A round of correctness fixes and a structural-debt cleanup round it out.

### Added

- **`autoSync` config** on `Smugglr.init()`: empty-state hydration on init plus sync-on-reconnect when the browser fires `online`. Multi-tab safe via `navigator.locks` (one tab runs, the rest wait). Exponential backoff with jitter on failure, capped at 5 min. No-op in Node. `s.stopAutoSync()` cancels the loop. (#113)
- **Optional `dest` in `Smugglr.init()`**: omit `dest` to run with no network at all -- nothing leaves the device. `.push()` / `.sync()` throw with a clear error; `.diff()` still reports local rows. Foundation for the "let users try the app before signing up" flow. (#111)
- **`updateAuth(token)` and `updateDest(dest)`** for runtime endpoint changes: rotate the dest auth token without re-initializing the WASM module or losing the metadata cache; replace the entire dest endpoint (URL, profile, token) for the anonymous -> account upgrade path. Source cache survives a dest swap. (#112)
- **`eraseLocal()`** GDPR / right-to-erasure helper: empties every configured sync table on the local SQLite database and clears smugglr's in-memory caches. Schema and non-synced tables untouched; dest is not contacted (server-side erasure is the app's concern). (#117)
- **`table-changed` reactive event**: subscribe via `s.on("table-changed", cb)`. Fires once per affected table after `pull` or `sync` completes the local write; `push` and `diff` never emit. Carries `{ table, changedPks, removedPks, source }`. The primitive that the framework binding plugins are built on. (#114)
- **`@smugglr/zustand`** middleware: wraps a Zustand store, hydrates from a smugglr-managed SQLite table on init, and re-pulls on `table-changed`. (#115)
- **`@smugglr/nanostores`** adapter: same shape for nanostores -- a writable atom backed by a smugglr table, kept fresh by sync events. (#116)
- **Local SQLite DataSource for browser (OPFS)** via `wa-sqlite`: `Smugglr.init({ source: { type: "local", executor: createWaSqliteExecutor(...) }, ... })`. Real SQLite in the browser, content-hashed delta against any HTTP-SQL backend. Generic `SqlExecutor` contract -- better-sqlite3 in Node, sql.js, or your own works too. Playwright e2e suite covers the full local-OPFS path. (#97)
- **Runnable examples set** under `docs/examples/`: CLI (D1, LAN broadcast), Node (server-to-D1, auto-sync), Rust (custom DataSource, tokio service), browser (OPFS + Turso, IndexedDB + Turso). Each runs as written from a fresh clone. (#119)
- **Masterless multicast LAN sync**: `smugglr broadcast` is now true masterless UDP multicast gossip -- every node multicasts a `primary_key -> content_hash` digest, peers pull divergence, rows ride multicast and apply idempotently (last-received-wins), late joiners reconcile via the heartbeat. Two or two hundred nodes on a subnet converge with no coordinator (O(N), replacing the previous pairwise-TCP discovery+exchange). Membership is key possession: nodes with the shared key sync regardless of where each stores its database file (no path-based scoping). The delta wire-format primitives and peer-discovery types remain available as an embedder API. **Wire `PROTOCOL_VERSION` is 3; nodes on other versions version-skip.** v0.1 limits: concurrent same-PK divergence resolves silently (no CRDT); deletes via the live delta path only. (#133)
- **`http-sql` target profile**: a built-in profile for any endpoint speaking the http-sql v0.1 spec (`{sql, params}` request, `{columns, rows}` response). Select with `profile = "http-sql"`; shared by the native plugin and the browser fetch adapter. (#131)
- **`${VAR}` expansion in `config.toml`**: string values expand `${NAME}` and `${NAME:-default}` from the environment at load time, so secrets (D1 tokens, stash keys, the broadcast key) come from the environment instead of the file. Unset with no default errors with the variable named; `$$` escapes a literal `$`. Expansion runs post-parse on the TOML value tree, so a substituted secret can neither inject TOML structure nor leak into a parse error. (#136)
- **Automatic retry with backoff on the write path**: transient upsert failures (HTTP 5xx / network / timeout) retry per `[sync]` config (`max_retries`, `initial_retry_delay_ms`, `max_retry_delay_ms`, `backoff_multiplier`); deterministic errors (4xx, bad SQL) fail fast; exhaustion exits 3. A server `Retry-After` is honored, capped by `max_retry_delay_ms`. (#137)

### Changed

- **WASM binary size**: release profile with `wasm-opt` cuts `smugglr_wasm_bg.wasm` from ~1.2 MB to ~277 KB compressed (~75% reduction). No API change. (#110)
- **Removed the orphaned in-process TCP sync transport** (~955 LOC): it was never wired into a shipping command. LAN sync is masterless multicast (#133); the delta wire primitives and peer-discovery types stay as an embedder API. (#145)
- Internal structural-debt cleanup across core, CLI, and the WASM adapters -- dead-code removal plus deduplication (config retry fields, snapshot structs, request-format arms, table-name validation, CLI command/output plumbing, shared WASM adapter helpers). No API or behavior change. (#146, #147, #148, #149, #150, #151, #152)

### Fixed

- Plugin lookup searches `~/.smugglr/plugins/` -- it was `~/.smuggler/` (a typo), which broke name-based plugin resolution. (#140)
- WASM content-hash honors glob `exclude_columns` patterns (e.g. `*_embedding`), matching transfer-time stripping; previously only exact column names were excluded, so glob-excluded columns produced phantom `content_differs`. (#141)
- WASM `conflict_resolution` errors on an unknown value instead of silently falling back to `local_wins` (which could sync the wrong direction). (#142)
- Conflict-skip warnings (`newer_wins` / `uuid_v7_wins` with no usable tiebreaker) fire in every sync direction, once per table -- a pull-only run previously gave no warning. (#144)
- Multicast deltas reserve wire-envelope + AEAD headroom when split, so a sealed delta part cannot exceed the safe datagram size. (#143)

### Notes

The 0.3.1 -> 0.3.3 patch releases were release-infrastructure only (crate metadata, npm README pointing at smugglr.dev, CI pnpm version + cache pins). No user-visible runtime changes.

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
