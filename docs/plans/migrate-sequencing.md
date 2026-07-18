# migrate 0.5.0 -- build sequencing & merge-queue plan

**Status:** planning -- validated by a 27-agent code-grounded review (2026-07-18)
**Scope:** the 21 open migrate + core-fix issues (#268-#282, #289-#293, #296).
**Companion:** `docs/plans/migration.md` (the design).

## Why this doc

GitHub's merge queue merges PRs **serially against the tip**. Two distinct concerns, and
only one of them is a queue-enforced *stopper*:

- **Queue-enforced (the real stoppers): co-queue collisions.** Two PRs that each pass alone
  but, combined against the tip, either conflict on a shared file (the second is ejected) or
  fail CI. The queue forces these on me; I avoid them by not letting the two be in flight
  together.
- **Self-enforced (not a stopper): dependency order.** A dependent PR must not be queued
  before its dependency has *merged* -- but I control when I queue, so the phase order
  prevents this by construction.

**Squash merge is on.** It does not relax the collision lanes (the queue still builds each
branch against the tip). It does mean: **do not stack PRs** (squash rewrites SHAs). Keep every
PR independent off `main`, and merge a phase's foundation before *opening* its dependents.

So: the phase order handles dependencies; the **collision lanes** handle the co-queue
stoppers. This doc gives both, the 0.5.0 cut line, and the residual risks.

## Dependency phases

Files in **bold** are new (no collision). Deps are issue numbers that must merge first.

### Phase 0 -- independent core fixes (no migrate deps; live bugs -- ship first)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #268 | first-run PK check -- **WARN in 0.5.0**, refuse in 0.5.x | **`pk_check.rs`**, `lib.rs` | none (serialize before #271) |
| #292 | blob hex/base64 convergence | `rowhash.rs`, `local.rs`, wasm `adapter_common.rs`, http-sql `adapter.rs`, `Cargo.toml` (+base64) | none |
| #269 | dup-`__pk` warn -> refuse | 3 builders (`local.rs`, `adapter_common.rs`, `adapter.rs`), `config.rs`, `error.rs` | none (after #292) |
| #293 | excluded-col newer-`updated_at` (new `converge_columns` class) | `diff.rs`/`sync.rs`/`multicast.rs`, `config.rs` | none (after #269) |

### Phase 1 -- migrate foundations (the spine's root)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #271 | manifest + structured Op enum + **native-only** envelope | **`migrate/manifest.rs`** + pre-stub the whole `migrate/` tree, `lib.rs`, `error.rs` (Migrate bridge) | #268 (lib.rs order) |
| #272 | ledger (+ nullable `preimage_ref`, `schema_projection` columns up front) | **`migrate/ledger.rs`**, `config.rs`, `error.rs` | #271 |

### Phase 2 -- apply / reverse / lint / author spine
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #275 | destructive-lint | **`migrate/lint.rs`** | #271 only |
| #273 | forward apply engine -- **LOCAL only** (remote = pure generators) | **`migrate/apply.rs`** (ledger-free; exposes `apply_ops`) | #271, #272 |
| #270 | rails-style CLI generator | **`migrate/generator.rs`**, **`migrate_cli.rs`**, `main.rs` | #271 |
| #274 | reverse/rollback (append-only vN+1; delta-scoped pre-image via `stash`) | **`migrate/reverse.rs`** | #271, #272, #273, #275 |

### Phase 3 -- compose + the differentiators
| Issue | What | Files | Deps |
|-------|------|-------|------|
| **#296** | **apply-driver + `smugglr migrate apply`** (composes lint/capture/apply/ledger) | **`migrate/driver.rs`**, `migrate_cli.rs` | #272, #273, #274, #275, #270 |
| #289 | recovery: surgical log + `--paranoid` (`VACUUM INTO`) | **`migrate/log.rs`** (+ own sidecar DB) | #296, #272, #273, #274 |
| #290 | reconcile: schema-drift | **`migrate/schema_projection.rs`** + **`reconcile.rs`** | #296, #271, #272 |

### Phase 4 -- convergence / multi-node (DEFER to 0.5.x)
#277 canonicalizer (+impl-hash binding), #276 authoring discipline, #278 version-gate
row-sync + quiesce, #279 identity-minting fills.

### Phase 5 -- onboarding & library path (DEFER to 0.5.x)
#281 FORK (declarative vs delta, needs mail), #280 int->UUIDv7 (gates #268's 0.5.x
refuse-flip), #282 parser fidelity, #291 embedder API (remote apply re-homes here).

## The 0.5.0 cut line

**HOLDS at 13 spine nodes** (Phases 0-3, including the new #296 driver). Ship: PK-safety
(warn) -> author -> apply (local) -> **reverse** -> **compose+apply** -> **recover** ->
**reconcile**, single-node, with the live core bugs fixed. Both founding value props
(surgical Reverse, drift Reconcile) land -- and they are only non-hollow *because* #296 writes
the ledger on a real apply.

**Defer to 0.5.x:** Phases 4-5. Remote apply (D1/Turso/rqlite transport) is NOT runnable in
0.5.0 -- #273 ships those as pure, unit-tested statement generators; transport is #291.

**Critical path:** #271 -> #272 -> #273 -> #274 -> **#296** -> #289. #275 and #270 are
side-prerequisites converging at #296 (#275 -> #274; #270 -> #296's CLI). #290 branches after
#296. (My pre-ultraplan path omitted the #296 driver.)

## Merge-queue collision lanes (validated against real code)

Never let two in a lane co-queue; land them in the listed order.

| Shared file | Issues | Rule |
|-------------|--------|------|
| the 3 metadata builders (`local.rs`, `adapter_common.rs`, `adapter.rs`) | #292, #269 | serialize; #292 first, #269 flips all three to `Result` in lockstep |
| `config.rs` `SyncConfig`+`Default` | #269 (`duplicate_pk`), #293 (`converge_columns`), #272 (`default_exclude_tables`) | serialize the two field-adders (#269, #293); #272 rebases |
| `diff.rs`/`sync.rs`/`multicast.rs` | #293, #278(deferred) | #293 in spine; #278 later |
| `rowhash.rs` | #292, #277(deferred) | #292 first; #277 freezes the hash version #292 mutates -- never invert |
| `error.rs` `SyncError`+`exit_code()` | #269, #271(Migrate bridge), #272, #290 | append-only; **land the Migrate bridge in #271**; one owner reconciles the exit-code numbers (multiple want 4) |
| `lib.rs` mod block | #268 (`pk_check`, top-level), #271 (`migrate`) | serialize #268 before #271; see mitigation |
| `migrate_cli.rs` MigrateCommand enum | #270, #296, #274, #289, #290 | #270 first (creates it); route ALL migrate commands here, never `main.rs` |
| `migrate/driver.rs` success path | #296, #289 (write-ahead hook), #290 (baseline write) | #296 first; serialize #289/#290 |
| `Cargo.toml` (core) | #292 (base64) | only real spine toucher -- #271 makes NO change (native-only envelope), #280 deferred |

**Dissolved / defused from the pre-ultraplan draft:**
- `snapshot.rs` lane is GONE -- neither #274 nor #289 touches it (#274 uses `stash::build_store`; #289 uses a new `VACUUM INTO` in `log.rs`).
- The wasm32 chacha gate is NOT a hazard -- #271's envelope is native-only for 0.5.0 (zero `Cargo.toml`, no wasm32 clippy trigger); wasm/edge envelope defers with remote apply.

### The `lib.rs` / `migrate/mod.rs` mitigation

Add ONE `pub mod migrate;` to `lib.rs` in #271 (after #268's `pk_check`), then pre-declare the
whole tree with its cfg gates as empty stubs in `migrate/mod.rs`: manifest, ledger, apply,
reverse, lint, log, reconcile, schema_projection, generator, convert, and a native-gated
`driver` (#296). Later PRs fill a body only and never re-touch `lib.rs` or `migrate/mod.rs`.

### Dependency order (self-enforced)

#268 before #271 | #271+#272 before #273 | #273+#275 before #274 | #274 before #296 |
#296 before #289/#290 | #281 before #282.

## Residual risks (from the adversarial pass)

- **#268 warn, not refuse (Sean's ruling).** 0.5.0 warns on legacy integer-PK instead of
  refusing, because the remedy (#280) is deferred. A warned user who proceeds can still hit
  the cross-node data loss the check exists to prevent -- the warning must be unmissable and
  carry the manual UUIDv7 recipe; the hard refusal flips on in 0.5.x with #280.
- **#269 covers only the builder path.** The multicast gossip `on_delta` path (`local.rs:302`,
  `INSERT OR REPLACE`) bypasses the builders -- a duplicate `__pk` arriving as a Delta is not
  caught until #278 (deferred). Changelog must not claim full cross-node enforcement.
- **#293 is inert until config migration.** The fix applies to the new `converge_columns`
  class; deployments using `exclude_columns` for pii stay buggy until operators move columns.
  Needs an explicit changelog config-migration note.
- **#274 Inline pre-image is unbounded.** Pin the size boundary where Inline refuses and a
  relay store becomes mandatory, or a large DROP-COLUMN silently fails at capture.
- **#289 two-file non-atomicity.** The sidecar log and the migrated DB are separate files;
  recovery treats pending as maybe-applied and leans on #273 idempotent redo; `--paranoid`
  restore must close all migrated-DB handles before the file swap or it corrupts an open WAL.
- **#296 is the sole apply path.** #291's future programmatic applier must build ON
  `apply_migration`, not fork a second loop, or the two drift on ledger/lint/capture order.

## The reconcile finding (informs #290 and #271)

A schema-drift hash must be a **pragma-derived semantic projection** (`table_info` +
`foreign_key_list` + `index_list`/`index_info`), **not** a hash of `sqlite_master` text. A
12-step rebuild that recreates a logically-identical schema rewrites the stored `CREATE` text
(`ALTER ... RENAME TO` quotes the name, reorders) -- so a raw or whitespace-normalized text
hash false-positives drift on every rebuild. The semantic projection is stable across
rebuilds yet still catches out-of-band `ALTER`/`DROP INDEX`, and is PRAGMA-portable to
D1/Turso. #271's `target_schema` precondition has the same latent bug and reuses the
projection. (Toy-verified 2026-07-18; reflection `019f766a`. Indexes canonicalized by
(cols, unique, origin), never the generated index name.)
