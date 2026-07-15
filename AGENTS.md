# AGENTS.md

Instructions for any agent working in the **smugglr** repo — a SQLite content-hashed delta-sync engine (Rust core + WASM/npm client + http-sql plugin). Tool-neutral; the legion-memory version of this contract is `legion whatami --repo smugglr`.

## Navigation: how to find code (read this first)

Order: **recall → sym → read → grep.** `grep` / `find` / `ls -R` are the last resort, mostly retired — and in the agent sandbox they are blocked outright.

`legion sym` is the cheap default: an in-process SCIP index, tiny payloads, never touches the read cache. Run `legion index <repo>` first and verify against a fresh index (`legion index --status`; `def --json []` is a reliable *absence* proof only when the index is current). The full surface — not just def/refs:

| Need | Use |
|------|-----|
| what's defined here (retires `grep "fn "`) | `legion sym list` |
| file inventory (retires `find` / `ls -R` / `tree`) | `legion sym tree` |
| who calls X | `legion sym refs` |
| who imports a file / what a file imports | `legion sym importers` / `legion sym imports` |
| what implements a trait | `legion sym impl` |
| signature + docstring | `legion sym hover` |
| non-code files (docs, config, css, prose) | `legion sym etc` |
| a diff's ref-count blast radius, at review time | `legion sym impact` |

Scope `--repo` and `--lang`, use `--json`. Exhaust sym before any `Read` (Read loads whole files into context and hits the cache; sym does not). When exploration must be delegated, spawn `subagent_type: legion:legion-explore` (sym-first by design) — never a generic Explore, which is grep/glob-oriented, has neither here, and strands itself.

**Canonical doctrine (cite this):** legion reflection `019f67d3-3881-7f22-bc87-58ecbe8e8426` — `legion recall --repo smugglr --context "how to navigate code"`.

## Doctrine (hard line)

Plan → Issue → Build → Simplify → Review → Fix → PR. **Simplify AND review every PR, no matter how small** — smallness is never a proxy for low risk; the small "obvious" diffs are exactly where a shipped-wire regression or an untested security claim slips through. Never record a quality gate clean without doing the real review pass.

## What smugglr refuses

- **WAL** — never (safe for a sole writer, fatal as a default; the community dismisses tools that touch `journal_mode`).
- **Crypto in the user's surface** — invisible; the social gesture is primary.
- **Its own state in the user's database** — cursors, config, PID live in smugglr's space, never the user's schema.
- **Feature flags standing in for plugins** — a new target is a plugin plus a TOML profile, never a flag in core.
- **Marketing claims not yet shipped.**

## Language & build

- **pnpm only** (never npm / yarn / npx) for JS/TS.
- **No `any`** — narrow from `unknown`.
- **UUIDv7** for IDs.
- **No emoji** in code, comments, commits, or docs.
- **Rust MSRV 1.75** — stable `AsyncFn` closures (1.85) are unavailable; prefer an enum over a boxed closure where a `Send` future matters.
- **`smugglr-wasm` is `#![cfg(target_arch = "wasm32")]`** — host `cargo test` / `clippy` compile it to nothing. Verify wasm code with `cargo clippy -p smugglr-wasm --target wasm32-unknown-unknown --all-targets -- -D warnings` and `wasm-pack test --node`.

## Git & quality gates

Branch first; never commit or push to `main`. The full pipeline before merge: build → simplify gate → pr-write gate → independent review → verify. Gates are HEAD-keyed and real — a clean gate means an accepted per-file articulation exists, not that someone typed "clean." (Moving to `legion push` over `git push` once legion #795 lands.)

## Coordination (legion)

`legion reflect`, never local markdown memory. Signals are pings, capped at 280 chars — long content via `legion post`. Read all signals before acting on any. Recall before grep, consult before reinvent. Verify every infrastructure claim (metric, method count, bundle size) with a `file:line` or a measurement, never plausibility.
