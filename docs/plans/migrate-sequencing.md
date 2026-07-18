# migrate 0.5.0 -- build sequencing & merge-queue plan

**Status:** planning
**Scope:** the 20 open migrate + core-fix issues (#268-#282, #289-#293).
**Companion:** `docs/plans/migration.md` (the design).

## Why this doc

GitHub's merge queue merges PRs **serially against the tip**. Two distinct concerns, and
only one of them is a queue-enforced *stopper*:

- **Queue-enforced (the real stoppers): co-queue collisions.** Two PRs that each pass alone
  but, combined against the tip, either conflict on a shared file (the second is ejected) or
  fail CI (a change one makes breaks the other). The queue forces these on me; I can only
  avoid them by not letting the two be in flight together.
- **Self-enforced (not a stopper): dependency order.** A dependent PR must not be queued
  before the code it needs has *merged* -- but I control when I queue, so the phase order
  prevents this by construction. It is discipline, not a hazard the queue imposes.

So: the phase order handles dependencies (mine to enforce); the **collision lanes** handle
the co-queue stoppers (the queue's to enforce). This doc gives both, plus the 0.5.0 cut line.

**Squash merge is on.** Squash does **not** relax the collision lanes -- the queue still
builds each branch against the tip, so a shared-file conflict still ejects the PR. What it
does add: **do not stack PRs.** Squash rewrites SHAs on merge, so a Phase-2 branch cut from
an unmerged Phase-1 branch will churn painfully. Keep every PR independent off `main`, and
merge a phase's foundation (e.g. #271/#272) before *opening* the PRs that depend on it. One
commit per issue on `main` -- clean bisect, but no stacking.

## Dependency phases

Files in **bold** are new (no collision). Deps are issue numbers that must merge first.

### Phase 0 -- independent core fixes (no migrate deps; live bugs -- ship first)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #268 | first-run PK sanity check | **`pk_check.rs`** | none |
| #292 | blob hex/base64 convergence | `rowhash.rs`, wasm adapters | none |
| #293 | excluded-col newer-`updated_at` | `diff.rs`, `config.rs` | none |
| #269 | dup-`__pk` warn -> refuse | `diff.rs`/`sync.rs`, `adapter_common.rs` | none |

### Phase 1 -- migrate foundations (the spine's root)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #271 | manifest + AEAD envelope | **`migrate/manifest.rs`**, `lib.rs`, `Cargo.toml` (un-gate chacha wasm32) | none |
| #272 | ledger | **`migrate/ledger.rs`**, `lib.rs` | #271 |

Extract the **shared schema-projection helper** here (used by #290 reconcile and #271's
`target_schema`) -- see the reconcile finding below.

### Phase 2 -- apply / reverse / lint / author spine
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #275 | destructive-lint | **`migrate/lint.rs`** | #271 |
| #273 | forward apply engine | **`migrate/apply.rs`**, http-sql plugin | #271, #272 |
| #274 | reverse/rollback | **`migrate/reverse.rs`**, `snapshot.rs` | #271, #275, #273 |
| #270 | rails-style CLI generator | **`migrate/generator`**, `main.rs` | #271 |

### Phase 3 -- the differentiators (surgical recovery + drift)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #289 | recovery: surgical log + `--paranoid` | **`migrate/log.rs`**, `snapshot.rs` | #272, #274, #275 |
| #290 | reconcile: schema-drift | **`migrate/reconcile.rs`** + projection helper | #271, #272 |

### Phase 4 -- convergence / multi-node (DEFER to 0.5.x)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #277 | canonicalizer (+impl-hash binding) | `rowhash.rs`, ledger version row | #272 |
| #276 | authoring discipline | generator / manifest ops | #270, #271 |
| #278 | version-gate row-sync + quiesce | `diff.rs`/`sync.rs` | #272 |
| #279 | identity-minting fills | apply / ledger | #272, #273 |

### Phase 5 -- onboarding & library path (DEFER to 0.5.x)
| Issue | What | Files | Deps |
|-------|------|-------|------|
| #281 | FORK: declarative vs delta (decision) | none (needs mail input) | -- |
| #280 | int->UUIDv7 conversion | **`migrate/convert.rs`**, `Cargo.toml` (uuid) | #273, #274, #275 |
| #282 | parser fidelity (quoted idents) | manifest/apply diff | #281, #271 |
| #291 | embedder library API | **`migrate/` public surface** | #272, #273 |

## The 0.5.0 cut line

**Ship in 0.5.0 (thin single-node spine):** Phases 0 + 1 + 2 + 3.
That delivers the whole headline loop -- PK-safety -> author -> apply (lint + ledger) ->
**reverse** -> **recover** -> **reconcile** -- single-node, with the live core bugs fixed.
The two founding value props (surgical Reverse, drift Reconcile) both land.

**Defer to 0.5.x:** Phase 4 (convergence / multi-node) and Phase 5 (the int->UUIDv7
flagship, the declarative/library fork, the embedder API). None of these are needed for a
credible single-node 0.5.0, and the flagship conversion (#280) is the hairiest class --
it wants the full pre-image + hash-rewrite machinery stable first.

**Critical path (longest chain):** #271 -> #272 -> #273 -> #274 -> #289. Five deep.
#290 and #270 branch off after #271/#272 and run in parallel with the #273->#274->#289 chain.

## Merge-queue collision lanes

Never let two of these co-queue; land them in the listed order.

| Shared file | Issues | Rule |
|-------------|--------|------|
| `rowhash.rs` | #292, #277 | #292 first (Phase 0), #277 later (Phase 4) -- naturally separated |
| `diff.rs`/`sync.rs` | #293, #269, #278 | serialize; one in the queue at a time |
| `config.rs` | #293, (#278) | minor; follows the `diff.rs` lane |
| `snapshot.rs` | #274, #289 | #274 before #289 (already the dep order) |
| `adapter_common.rs` (wasm) | #269, #292 | serialize (both render values) |
| `main.rs` (CLI) | #270 + apply/reverse/reconcile/`--paranoid` command PRs | serialize CLI PRs |
| `Cargo.toml` (core) | #271 (chacha wasm32), #280 (uuid) | trivial rebase; low risk |
| **`lib.rs`** | **every new-migrate-module PR** | see the mitigation below |

### The `lib.rs` hazard (the most likely queue-stopper)

Almost every migrate PR adds a `mod <name>;` line to `smugglr-core/src/lib.rs`. In a merge
queue that means the second migrate PR in flight conflicts on `lib.rs` -- constantly.

**Mitigation:** in the Phase-1 foundation PR (#271), pre-declare the whole migrate module
tree as empty stubs:

```rust
// smugglr-core/src/lib.rs
mod migrate; // -> migrate/mod.rs declares: manifest, ledger, apply, reverse,
             //    lint, log, reconcile, convert, generator (empty stubs)
```

Then every later PR only fills in its own **new file body** and never re-touches `lib.rs`
(or `migrate/mod.rs` only for its own line). `lib.rs` stops being a shared surface and the
lane collapses.

### The wasm32 gate (#271)

#271 un-gates `chacha20poly1305` for `wasm32`, and CI clippy is gated on the wasm32 target.
If that PR is not clean on wasm32 it blocks the **entire** queue behind it. Treat #271 as a
solo, extra-reviewed merge -- do not queue anything behind it until it is green.

### Dependency order (self-enforced -- queue each only after its dep has *merged*)

Not queue-stoppers (I control queueing), but the sequence to hold:
#271+#272 before #273 | #273 before #274 | #274 before #289 | #272 before #276/#279
| #281 before #282.

## The reconcile finding (informs #290 and #271)

A schema-drift hash must be a **pragma-derived semantic projection** (`table_info` +
`foreign_key_list` + `index_list`/`index_info`), **not** a hash of `sqlite_master` text. A
12-step rebuild that recreates a logically-identical schema rewrites the stored `CREATE`
text (`ALTER ... RENAME TO` quotes the name, reorders) -- so a raw or whitespace-normalized
text hash false-positives drift on every rebuild. The semantic projection is stable across
rebuilds yet still catches out-of-band `ALTER`/`DROP INDEX`, and is PRAGMA-portable to
D1/Turso. #271's `target_schema` precondition has the same latent bug and must reuse the
projection. (Toy-verified 2026-07-18; reflection `019f766a`.)
