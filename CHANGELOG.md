# Changelog

## 0.5.0 (2026-08-12)

`smugglr migrate` lands: envelope-based, self-reversing schema migrations for the SQLite family, built as seven pieces that each hold on their own -- a manifest and structured op enum, a rails-style generator, a tamper-evident ledger, a forward apply engine, faithful reverse/rollback, a destructive-op lint, and a first-run primary-key compatibility check. Alongside it, one silent sync bug is fixed (multicast applied remote rows without consulting the conflict policy the crate already declared), blob columns are canonicalized in the content hash, and the release pipeline is hardened so a single crate can no longer take the whole publish down.

**Note for crates.io consumers.** 0.4.3 was tagged but never published -- its `publish-crates` job failed on the first step and none of the five crates moved. Upgrading a crate dependency from 0.4.2 lands both releases at once, including 0.4.3's internal sweep. One item there is a breaking signature change: `Multicast::recv_and_handle` takes a caller-supplied `&mut [u8]` buffer as its first argument (#211). npm consumers are unaffected -- 0.4.3 published normally there.

### Added

- **`smugglr migrate`** -- schema migrations that cross the same boundary sync does, with the globally-unique-primary-key precondition the engine already assumes.
  - **Migration manifest and structured `Op` enum**, with a native envelope format instead of raw SQL strings, so a migration is inspectable before it is applied. (#271)
  - **Rails-style generator** -- `smugglr migrate generate` scaffolds a timestamped migration from the CLI. (#270)
  - **Migration ledger** -- version-gated, success-gated, and tamper-evident, so a partially applied or edited-after-the-fact migration is detected rather than replayed. (#272)
  - **Forward apply engine** with idempotent per-op DDL and pure remote generators, so re-running an interrupted apply converges instead of double-applying. (#273)
  - **Reverse/rollback** with faithful verbatim-DDL reverse and a delta-scoped pre-image, so a rollback restores what was actually there rather than a reconstruction of it. (#274)
  - **Destructive-op lint** -- two-axis op classification with a pre-image gate, so a destructive migration must declare that it is one. (#275)
  - **First-run primary-key compatibility check**, warning in 0.5.0, for tables whose keys do not satisfy the migrate precondition. (#268)

### Fixed

- **Multicast apply is ordering-aware.** smugglr-core resolved same-primary-key conflicts two different ways depending on transport, and the LAN one was blind: the remote path honors `ConflictResolution` and `timestamp_column`, while the multicast path read `timestamp_column` only to build the digest, discarded it, and applied with a bare `INSERT OR REPLACE`. A stale peer row silently overwrote a newer local one. Resolution now rides inside the write as a single atomic `ON CONFLICT ... DO UPDATE ... WHERE` per row, with no read-modify-write. Two deliberate choices: the policy lands on `BroadcastConfig` and is **not** inherited from `[sync].conflict_resolution` -- that field defaults to `LocalWins` while multicast has always behaved as `RemoteWins`, so inheriting it would flip every existing deployment to never accepting a peer row, a convergence break shipped as a bugfix. And the ordering signal is a column *list* reduced with max, not a single column, so a tombstone that stamps `deleted_at` without bumping `updated_at` is not a tie that loses the delete. A single-entry list degenerates to the old single-column behavior. (#310)
- **An edit confined to a hash-excluded column is no longer dropped, via a new `converge_columns` class.** Excluding a column from the content hash means a change confined to that column produces a hash MATCH -- and a hash match was the diff's skip condition, so the row was classified `identical` and never transferred, even with a newer `updated_at`. Silent data loss with no error, no conflict, and no bucket anyone inspects. The fix distinguishes two things `exclude_columns` was conflating: `exclude_columns` stays "out of the hash AND off the wire" (derived or huge values, embeddings being the motivating case), while the new `[sync].converge_columns` means "out of the hash but still synced". For any table with a `converge_columns` pattern, a hash match is no longer treated as proof of equality -- the diff falls through to comparing `timestamp_column` and takes the newer row. A tie resolves to `identical` rather than `content_differs` on purpose: equal hashes mean the hashed columns agree, so only unhashed ones could differ, and equal timestamps give no basis to prefer either side -- calling it a conflict would park the row in permanent warning noise. The hash-exclusion set (`exclude_columns` ∪ `converge_columns`) is computed in exactly one place and used by every hash producer -- the diff path, the multicast digest, and the wasm cached diff -- because two producers covering different column sets would hash identical rows differently and never converge, which is the #292 blob-encoding failure with a different cause. (#293)

  **A pattern in both lists is refused at config load.** Copying a pattern into `converge_columns` instead of moving it is silent destructive data loss, not an ambiguity: the column is excluded from the hash (the union covers both lists), the row is then selected for transfer on its newer timestamp, and the transfer strips it anyway because stripping honors `exclude_columns` alone -- so the row is sent without the one column that caused it to be sent. On `INSERT OR REPLACE` backends that is a DELETE+INSERT, so the destination's existing value is *nulled* rather than left stale; the native `ON CONFLICT DO UPDATE` path leaves it stale. Either way `updated_at` crossed, so the next sync sees matching hashes and tied timestamps and classifies the row `identical` forever. Every step reports success. `smugglr` now refuses such a config at load, naming both patterns, rather than picking a precedence -- exclude-wins keeps losing the edit, converge-wins silently transmits a column an operator may have excluded precisely so it would never leave the machine. Glob-vs-glob intersection is undecidable in general; the check catches identical patterns and a pattern that matches the other's text, and its limits are documented on `SyncConfig::validate_column_lists`.

  **Every `[broadcast]` peer must configure these lists identically.** The hash-exclusion set is local config and is never negotiated on the wire -- no handshake, no config fingerprint, and `PROTOCOL_VERSION` does not cover it. Two peers on the same group and table with different `converge_columns` hash the same row over different column sets, so their hashes never coincide: the digest advertises a hash the peer cannot match, the peer asks for the row on every heartbeat, and the mesh never quiesces. No error, no warning, permanent churn. This is not new to `converge_columns` -- `exclude_columns` has always fed the same hash and always had this property -- but it is stated now because this is the field that makes an operator think about the hash input for the first time, and because "converge" in the name invites the opposite assumption. Roll changes to either list out to every peer.

  **Config migration -- this fix is inert until you move a pattern.** Nothing reclassifies your columns for you. A deployment relying on `exclude_columns` for a column it actually wants synced (a PII column kept out of the hash, say) keeps losing those edits until an operator moves that pattern into `converge_columns`. The migration is deliberately manual: the two lists mean different things, and guessing which one a pattern wanted would silently start transmitting a column an operator may have excluded precisely so it would never leave the machine. Deployments with no `converge_columns` configured are byte-for-byte unchanged and pay nothing -- the hash-match fast path stays on.

- **Blob columns fold to one canonical encoding in the content hash.** The native rusqlite path renders a blob as lowercase hex; the JSON SQL backends (Turso, rqlite, D1, and the wasm executors) commonly render standard base64. Two peers folding different renderings of the *same bytes* never converge -- the row reads `content_differs` on every sync, forever, with no error to point at. The content hash now pins one canonical form (lowercase hex), and a backend that renders otherwise declares its `BlobEncoding` and canonicalizes before hashing. Only explicitly-declared `BLOB` columns are canonicalized: a column with an empty declared type has BLOB affinity in SQLite but holds arbitrary dynamically-typed values, and base64-decoding a genuine text value there would corrupt it. Native-only deployments already emitted the canonical form and see no hash change. (#292, residual of #202)

### Changed

- **The release pipeline is idempotent and diagnostic.** `publish-crates` ran five sequential `cargo publish` steps with no guard, so any failure partway left the earlier crates published and the run unrepeatable -- exactly how 0.4.3 got stuck. Each step now skips a crate version already on the index and treats a lost race as success (`scripts/publish-crate.sh`). A new CI job asserts every inter-crate version req equals the workspace version, which is the one thing local builds structurally cannot catch: they resolve those deps by path and never read the version string that `cargo publish` actually ships (`scripts/check-crate-versions.py`). The test matrix no longer cancels its siblings on the first red, so a single-platform failure still returns a complete run.
- **`smugglr-wasm` is marked `publish = false`.** It ships to npm via wasm-pack and its `smugglr-core` dep is path-only, which `cargo publish` cannot express -- previously that was true but undeclared.

### Docs

- **Migration design and sequencing** -- drift-audit corrections folded into the design and sequencing docs (#304), and the D1 atomicity citation in decision 6 corrected after a `SPEC.md` S4.2 misread (#307).

## 0.4.3 (2026-07-17)

The last 0.4.x release before the 0.5.0 `smugglr migrate` work. One real correctness fix in the sync path -- composite primary keys with a `|` in a value no longer collide -- plus a numeric float-timestamp ordering fix, a large internal structural-debt sweep from the code audit, and the design groundwork for `smugglr migrate` landed as a doc.

### Fixed

- **Composite-primary-key `__pk` collision.** `pk_text_expr` joined composite key parts with a bare `|`, so two distinct keys could render to the same `__pk` (`{a:'x|', b:'y'}` and `{a:'x', b:'|y'}` both -> `x||y`) and silently collapse onto one entry in the change-metadata map -- lost rows and spurious deletes on any composite-PK table with a `|` in a text component. Each composite part is now delimiter-escaped before the join. Single-column PKs are unchanged; composite-PK tables re-render their `__pk` once and re-sync. (#285)
- **Float-serialized timestamps order numerically.** `compare_ts` gained a numeric tier so float-rendered `updated_at` values resolve conflicts in the right direction instead of by string order. (#241)
- **`watch --dry-run` labels its text output as a dry run** so a dry-run pass is not mistaken for an applied sync. (#218)

### Changed

- Internal structural-debt sweep from the code audit -- fourteen refactors, no API or behavior change: wire types unified into a no-tokio `smugglr-wire` crate (#228), `push_all`/`pull_all` behind one directional driver (#224), a shared `createPersistBinding` for the zustand and nanostores plugins (#227), `generate_batch_sql`/`rows_to_maps` hoisted into core (#222), dry-run watch opens the local DB read-only (#217), `is_transient_error` unified onto `is_retryable` (#226), SDK param extractors collapsed (#225), one home for the no-primary-key skip warning (#216), `cached_table_info` hoisted into `adapter_common` (#219), the recv buffer reused across datagrams (#211), the dead `batch.rs` removed (#212), the unreachable `column_glob_match` arm dropped (#214), and CI clippy gated on the wasm32 target (#155).

### Docs

- **Exit codes** are now surfaced in `smugglr --help` (0-5), matching `SyncError::exit_code`. (#266)
- **`AGENTS.md`** tool-neutral agent contract for the repo. (#264)
- **`smugglr migrate` design doc** (`docs/plans/migration.md`) -- envelope-based, self-reversing, SQLite-family migrations with the globally-unique-primary-key precondition; the foundation for the 0.5.0 migrate work. (#284)

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
