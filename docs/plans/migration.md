# smugglr migrate (v0.1 design)

**Status:** draft, in design
**Crate:** `@smugglr/safehouse` (houses migrate now; the parked GDPR crypto-shred layer when we return to it). `migrate` is the feature/command; the migration-tracking table keeps the name "ledger" (`@smugglr/ledger`).
**Author:** smugglr
**Research:** legion reflection chain `019f684a..019f698e`
(`legion recall --repo smugglr --context "smugglr migrate design"`)

## Goal

Versioned, reversible, encrypted schema-and-data migrations for the SQLite family,
built for the reality that an agent probably wrote the migration. One command applies
a migration to a local or remote SQLite-dialect target; one command reverses exactly
that migration and nothing else; and neither can silently nuke production.

The differentiator is not "we do migrations." Every ORM does migrations. The
differentiator is **surgical reversibility** and **drift reconciliation** on a
substrate that already content-hashes every row -- the two things the incumbents
(prisma, drizzle, supabase-cli) are worst at, and the two pains the discourse
complains about most.

## Motivation

**The founding pain.** smugglr exists because an AI coding agent kept corrupting a
drizzle migrations folder. That is not an anecdote; the eavesdrop discourse corpus
shows it is a market. Agent-authored-migration terror is the dominant theme
practitioners voice:

- "Now that AI agents write my DB migrations, I'm terrified of pushing one that nukes
  prod." (r/webdev)
- "Three AI coding agents deleted production databases in 16 months. All three shared
  one missing control." (r/devops)
- "The problem isn't that AI makes mistakes -- it's whether you can roll back."
  (r/devops)

The single most-cited hard pain is **rollback**: "the one that hits the hardest is
reverting a migration locally" (r/supabase), "the only thing drizzle lacks is
migration rollback" (r/nextjs), "lost count of how many times I had to reset the
database with prisma" (r/nextjs). Reset-the-whole-DB is the de-facto rollback. And
naive backup/restore is itself a footgun: "restore from backup reverted my project to
an 8-month-old state" (r/supabase).

**Why smugglr is uniquely positioned.** It already ships the exact primitives for "an
agent wrote this; undo exactly it, nothing else":

- content-hashed row deltas (it *knows* what a step changes),
- `snapshot` / `restore` (backup-before-migrate, and the pre-image store for reversal),
- `dry-run` (preview before apply),
- an XChaCha20-Poly1305 encryption envelope and a user-managed PSK model already in
  `smugglr-core::broadcast`.

No competitor holds these parts. migrate composes them.

**Founding-case nuance (huttspawn, the reference deployment).** The market's #1 pain is
*Reverse* (rollback). But the *founding* corruption was subtler and it reframes which
mechanism is the hero: huttspawn's break was not a missing down-migration -- it was the
reflex of hand-editing drizzle's `_journal.json` when the tool errored, causing a
**silent** desync. Rollback does not touch that (the corruption went *around* the tool,
not through it). Two other mechanisms do: **fail-closed integrity** (a tampered artifact
fails verification and migrate refuses -- intrinsic to the envelope, not an external
hook) and **Reconcile** (detecting `DB schema-hash != ledger expected hash` catches an
out-of-band edit). So: lead the *market* story with Reverse; lead the *founding* story
with integrity + Reconcile. Both are true; they serve different audiences.

## Non-goals

- **Cross-engine migration** (SQLite -> Postgres, MS SQL -> Postgres, Domo ->
  Snowflake). That is ETL / dialect-translation / type-mapping -- a different product
  that would dissolve smugglr's single-dialect advantage. Explicitly out.
- **ORM / schema authoring.** We own the migration *engine* (apply / reverse /
  reconcile), not the schema DSL or query builder. Keep your drizzle; we eat the part
  that keeps breaking. Precisely: migrate retires **drizzle-kit** (the migration
  folder / journal), never **drizzle-orm** (the query builder). This doc governs that
  distinction over any looser "retire drizzle" shorthand (flagged by platform and
  astro-data; @rafters/ledger's Drizzle adapter is therefore untouched).
- **Dev-collision migration ordering** (two developers minting the same migration
  number). Self-inflicted; not ours to solve.
- **fence migration governance.** fence stays a lean http-sql transport. It executes
  the plain SQL a migration produces and never knows a migration happened. No
  linting, provenance, or rollback-as-a-service in fence.
- **Zero-knowledge-against-fence storage.** Encryption fence cannot read turns fence
  from a queryable database into a blob store. A niche "encrypted vault" tier at most;
  not v0.1.
- **Whole-database at-rest encryption** (SQLCipher-style). That is the user's data
  surface, which smugglr refuses to touch. Our encryption is the transport envelope.
- **Key negotiation / rotation protocol.** Reuse the existing PSK model
  (`BroadcastConfig.secret`); rotation is a separate proposal if the threat model ever
  demands it.

## Scope: SQLite family, one dialect

Every smugglr target speaks the same SQLite SQL: local SQLite, Cloudflare D1,
Turso / libSQL, rqlite, SQLite Cloud. A migration therefore never *translates*. The
only per-target variance is **how a migration lands**, not **what SQL it is**:

| Target        | Apply quirk                                                      |
|---------------|-----------------------------------------------------------------|
| local SQLite  | `BEGIN..COMMIT`, transactional DDL, `PRAGMA foreign_keys=OFF`    |
| Cloudflare D1 | no explicit transactions -- batch API, `defer_foreign_keys=ON`  |
| Turso/libSQL  | extended `ALTER TABLE` -> fewer 12-step rebuilds; embedded replicas |
| rqlite        | Raft leader apply; bulk-transaction request                     |

One dialect, N apply-strategies. The variance is a bounded quirk set living in the
http-sql plugin profile per target -- not an open-ended translation problem.

## Precondition: globally-unique primary keys (a hard refusal)

smugglr's identity **is** the primary key -- masterless last-received-wins matches rows by PK.
A locally-sequential key (`AUTOINCREMENT`, or a bare `INTEGER PRIMARY KEY` rowid) means two
machines both mint `id = 5` for different rows, and the fabric silently overwrites one with the
other. Guaranteed data loss -- so smugglr does not merely warn, it **refuses**:

- **First-run sanity check.** smugglr inspects the schema on first run and refuses an
  incompatible one. A globally-unique BIGINT (snowflake / k-sortable) passes; `AUTOINCREMENT` /
  rowid does not. (BIGINT is not the crime; *locally-sequential assignment* is, and it is
  detectable from the schema.)
- **migrate does not allow old ideas.** It will not create locally-sequential keys, and it
  blocks a schema that keeps them. The sanctioned path forward is the **`int -> UUIDv7`
  conversion migration** -- the flagship onboarding case, and the hairiest migration class
  (rewrite every PK, cascade every FK, rewrite every content-hash: exactly what the envelope's
  pre-image / reversibility machinery exists for).
- **Rebuild preserves `sqlite_sequence`.** Any 12-step rebuild must carry the autoincrement
  high-water mark forward or future inserts reuse IDs -- a correctness bug even single-node,
  relevant to legacy tables mid-conversion.

This is line 1 of the README, too: no GUID-ish keys, no smugglr.

## The pain map (why this shape)

The migration problem stratifies into five layers. We do not try to win all five.

| Stratum        | The layer         | smugglr's stance                                    |
|----------------|-------------------|-----------------------------------------------------|
| 1. Author      | writing it        | **out** -- crowded, commodity (prisma/drizzle/alembic) |
| 2. Apply       | running it        | idempotent forward-only; partial-failure safe        |
| 3. Reverse     | undoing it        | **lead** -- surgical rollback, the #1 corpus pain    |
| 4. Reconcile   | env consistency   | **lead** -- drift is smugglr's founding pain          |
| 5. Trust       | is it safe?       | credibility layer -- destructive-lint, provenance     |

Lead with **Reverse** (rollback) and **Reconcile** (drift). Trust (linting) is the
credibility layer. We do not compete on Authoring.

## What we reuse vs build

**Reuse (already in `smugglr-core`):**

- XChaCha20-Poly1305 envelope -- `encrypt_packet` / `decrypt_packet`, 256-bit key,
  24-byte nonce + 16-byte tag (`broadcast.rs`; see `docs/plans/broadcast-tcp-encryption.md`).
- PSK model -- `BroadcastConfig.secret`, user-managed, smugglr never generates or
  stores it; "membership = key possession".
- SHA-256 content hashing (`sha2`, WASM-compatible) -- reused for the manifest checksum.
- `snapshot` / `restore` -- backup-before-migrate and the destructive-op pre-image store.
- `dry-run` -- the preview half of rollback.
- Idempotent last-received-wins apply + `ReplayGuard` + `DeltaPacket` from the
  multicast fabric (`docs/plans/masterless-multicast-sync.md`).

**Build:**

- The migration *manifest* (ordered deltas + reverse + metadata).
- Reverse-carry (delete the hand-written down-migration).
- The destructive-lint (detect lossy ops, gate pre-image capture, warn).
- The migration *ledger* (versioned, idempotent, converges like a table).
- The per-target apply-strategy dispatch.

## Design decisions

### 1. A migration is an envelope

A migration is one self-describing object: a **manifest** of ordered content-hashed
deltas, wrapped in an AEAD envelope. The same object does the whole lifecycle --
encapsulate, version, integrity, privacy, apply (forward), and reverse.

**Why:** the discourse's pains (no rollback, drift, cruft, malformed agent files) are
all symptoms of migrations being a loose *folder of timestamped SQL* with no integrity,
no intrinsic reverse, and no provenance. A single hashed, versioned object closes all
of those at once.

**Trade-off:** the object is heavier than a bare `.sql` file (it carries reverse data
and metadata). Additive migrations stay tiny; destructive ones pay for their pre-image
(decision 4).

### 2. Reuse the existing XChaCha20-Poly1305 envelope + PSK (do not invent crypto)

The envelope encryption is not a new subsystem. It is the `chacha20poly1305` crate
already vendored in `smugglr-core`, driven by the existing `encrypt_packet` /
`decrypt_packet` and the existing `secret` PSK.

**Why:** ChaCha20 over AES-GCM because smugglr's surface (WASM, edge, mobile) lacks
guaranteed AES-NI; ChaCha is fast and constant-time in software there. XChaCha20's
192-bit random nonce is collision-safe stateless, which the per-migration envelope
wants. And it is already written and tested -- operator confirmed prior art (ezmode
used the same envelope pattern).

**What the encryption actually buys, and why it is OPTIONAL (advisor + mail).** Both sync
paths are *already* XChaCha20-encrypted (masterless-multicast per-datagram; broadcast-TCP
per-frame). For a migration that only travels the live fabric, envelope confidentiality is
redundant with transport; its non-redundant value is **at-rest / untrusted-relay** (a
migration parked in `object_store`, a remote ledger table, a non-fabric fetch). And some
payloads must *not* be confidential: mail ships **public, open-source DDL** meant to be
read and copied -- ChaCha20 on it is worthless. So the split is: **integrity is always
on** (SHA-256 manifest + validated provenance -- "this is mail v1.2.3, unmodified");
**confidentiality (AEAD) is opt-in / rejectable**, and it belongs on the app's *data
pre-images*, not on public DDL. The "privacy stamp" is honest only where confidentiality
buys something the transport does not already give.

**Trade-off / note:** `chacha20poly1305` is currently gated behind the `native`
feature. Encrypting envelopes on the WASM/edge apply path requires un-gating it for
`wasm32`. Flagged as a build item.

### 3. The envelope carries the reverse

The envelope carries what is needed to undo itself. Rollback is applying the
envelope's reverse, not running a separately-authored down-migration.

Rollback is a **compensating forward migration**, not a transactional undo: the reverse
deltas apply as a *new ledgered step* (a version that inverts its predecessor), never an
in-place rewind. That is what keeps this consistent with forward-only (decision 6) --
there is only ever roll-*forward*; "rollback" is rolling forward through the inverse.

**Why:** this deletes Stratum 3's entire pain. Nobody writes a down-migration (the
thing prisma makes you hand-write, drizzle does not support, and everyone else "solves"
by resetting the DB). The down is *intrinsic*.

**Trade-off:** see decision 4 -- the reverse is free for additive ops and costs a
pre-image snapshot for destructive ones.

### 4. Reversibility classes: additive vs destructive

- **Additive** (`ADD COLUMN`, `CREATE TABLE/INDEX`): reverse = the structural inverse
  (drop what was added). No data carried. Envelope stays small.
- **Destructive / lossy** (`DROP COLUMN`, `DELETE`, type-narrowing, hash-rewriting
  transforms): reversible **only if the envelope captured the pre-image** -- a
  scoped `snapshot` of exactly what is about to be lost.

**Why:** reversible-by-construction is honest only if we admit destructive ops lose
data unless we snapshot it first. This is where `snapshot`/`restore` and content-hash
scoping pay off -- the reverse for a destructive op *is* a scoped snapshot.

**Trade-off:** destructive envelopes are larger (they carry pre-images). That is the
price of surgical reversibility, and the destructive-lint surfaces the cost before you
pay it.

### 5. The destructive-lint is load-bearing

A pre-apply linter classifies every op in the manifest: additive, destructive, or
hash-rewriting. Destructive ops trigger pre-image capture and a loud warning; the
apply refuses to proceed on a destructive op without an acknowledged, captured
pre-image.

**Why:** this is the safety rail the agent-authored reality demands, and it is what
makes decision 4 real rather than aspirational. Precedent: Postgres's MigrationSafe
("catch dangerous migrations before they hit production").

**Trade-off:** false positives (a "destructive" op the operator knows is safe) need an
explicit override. Better a loud false positive than a silent prod-nuke.

### 6. Safety without transactions

Design as if **no transaction exists**. D1 (a major target) has none, and the http-sql
spec's `atomic: true` is optional and server-rejectable (SPEC.md S4.2). So the primary
safety mechanism is **expand-contract + idempotent forward-only + `IF NOT EXISTS` +
ledger skip-if-applied**: every step is additive and re-runnable, so a half-applied
migration is safe to re-run.

**Why:** we cannot make correctness depend on a primitive half our targets lack. Local
SQLite's transactional DDL is a free bonus we use where present -- never the foundation.

**Trade-off:** expand-contract means some changes take two migrations (expand, then a
later contract) instead of one in-place edit. That is the accepted cost of surviving a
transactionless target. Forward-only and "carries-reverse" (decision 3) are one model,
not two: rollback is a compensating forward step (the inverse applied as a new version),
never a transactional undo.

**Concurrent-writer race (huttspawn, from lived D1 pain).** skip-if-applied covers
*re-run* -- the same node applying vN twice, a no-op. It does NOT by itself cover two
*different* writers racing the ledger insert against a transactionless D1, which can
double-apply (huttspawn hit exactly this: parallel `wrangler migrate apply` corrupts the
journal, and runs a wrangler-apply mutex today -- platform 2026-04-22). Needs a lease /
mutex / compare-and-swap on the ledger row. Open item -- see open-Q7.

### 7. The ledger is the load-bearing artifact (gate + tamper-evidence)

A ledger records each migration: version, checksum, applied-at, success, in a
**namespaced table** (`_smugglr_migrations`, so app introspection/reset tooling ignores
it -- mail's ask). It is the most load-bearing object in the design: **three independent
reviews (advisor, huttspawn, mail) converged on it.** It carries two invariants, not the
"just another syncable table" the first draft implied.

**Naming (operator ruling: keep "ledger", namespace it).** platform (agent-of-record for
the published `@rafters/ledger`) requested a rename to avoid an unscoped homonym. Sean's
ruling overrides: npm scopes exist for exactly this -- smugglr claims **`@smugglr/ledger`**,
a distinct scope from `@rafters/ledger`, and the on-disk table is already prefixed
(`_smugglr_migrations`). Scope + table-prefix disambiguate structurally, so smugglr keeps
"ledger". In cross-repo *prose*, scope-qualify ("smugglr's ledger" / `@smugglr/ledger`) to
avoid ambiguity. Interaction with `@rafters/ledger` remains coincidental shape-convergence
only -- disjoint tables at different layers (this = DDL apply-state, control-plane; theirs
= DML row-audit/GDPR, data-plane), zero runtime contention.

**Invariant A -- row-sync gates on ledger-version equality (advisor).** Two peers
exchange row deltas only when their ledger versions match; a node at vN does not accept
row upserts from, or apply deltas to, a peer at vM != N.

*Why (the version-skew storm):* a hash-rewriting migration at vN makes A's row
content-hashes differ from a vN-1 node B for every affected row. Without the gate, the
next heartbeat reads that as full-table data divergence -- B pulls rows for a column its
schema lacks; A pulls B's stale rows and re-applies at vN; they flip-flop.
last-received-wins cannot distinguish a schema-version artifact from real drift. The gate
pauses row-sync between skewed peers until the ledger levels them.

*Consequence (stated, not hidden):* a migration rollout **partitions the fabric by
version** until it propagates -- row-convergence across "2 to 200 nodes" stalls between
skewed peers mid-migration and resumes once level. Correctness over availability during
the rollout window.

**Invariant B -- the ledger is chain-hashed / tamper-evident (huttspawn).** Each entry
includes the hash of the prior entry. Any out-of-band `UPDATE`/`DELETE` breaks the chain,
and migrate detects it on the next run and refuses.

*Why:* the ledger is the new `_journal.json`. huttspawn's founding corruption was
hand-editing drizzle's journal into silent desync. A plain hand-editable ledger
resurrects that exact reflex; chain-hashing turns a silent out-of-band edit into a loud,
detected failure. Tamper-evidence on an artifact we already have -- no new subsystem.

**Trade-off:** the ledger is now the critical path -- ordered, reliable, tamper-evident,
and concurrency-safe (open-Q7). Stricter than the unordered row fabric it sits above, and
the right place to spend the strictness.

### 8. Per-target apply strategies

One dialect, N apply-strategies, dispatched by the http-sql plugin profile: local uses
`BEGIN..COMMIT` + `foreign_keys=OFF`; D1 uses the batch API + `defer_foreign_keys`;
Turso uses extended `ALTER` to skip 12-step rebuilds where possible; rqlite uses a bulk
Raft transaction.

**Why:** the SQL is identical; only the transaction/DDL mechanics differ per host. That
variance is bounded and belongs in the adapter, not the migration.

### 9. The envelope rides above http-sql; http-sql and fence stay untouched

The envelope lives at smugglr's own sync/distribution layer. A client decrypts it,
verifies integrity, runs the destructive-lint, then emits **plain http-sql batches** to
apply. The http-sql server (including fence) only ever sees plaintext SQL it executes.

**Why:** an http-sql server *executes* the SQL, so it must read plaintext -- you cannot
encrypt end-to-end and still have D1/Turso run it. The strong, host-blind privacy stamp
therefore lives one layer up, in smugglr's distribution channel, not in the http-sql
spec. This keeps http-sql minimal (its design ethos) and fence lean.

**Trade-off:** "encryption in the http-sql spec" as a marketable line is off the table
(it would be a weak, non-host-blind stamp). Acceptable -- the real stamp is the
smugglr-layer envelope.

### 10. Manifest integrity via SHA-256

The manifest is checksummed with SHA-256 -- the same `sha2` content hash smugglr
already uses for change detection. AEAD (decision 2) provides tamper-detection on the
wire; the SHA-256 manifest checksum provides a stable content identity for the ledger
and for skip-if-applied.

**Why:** reuse the one hash smugglr already depends on (WASM-compatible), rather than
add BLAKE3 for a structural precedent (Filepack) whose algorithm choice we do not need.

## Envelope / manifest format (sketch, not final)

```
Envelope  = XChaCha20-Poly1305( manifest_json, PSK )   // reuses encrypt_packet
manifest_json = {
  "version":        <monotonic migration version>,
  "checksum":       <SHA-256 of the canonical manifest body>,
  "target_schema":  <SHA-256 of the schema this migration expects to apply against>,
  "up":   [ <ordered delta ops> ],        // forward
  "down": [ <ordered inverse ops> ],       // reverse (structural inverse for additive)
  "preimage": <snapshot ref | inline, present only for destructive ops>,
  "flags": { "destructive": <bool>, "hash_rewriting": <bool> },
  "author": <provenance id>                // v0.1 informational; signature = later layer
}
```

Carried as a new payload type over the existing encrypted fabric (a new `Msg` variant
alongside `Digest` / `Want` / `Delta`, or a dedicated migration flow -- open question).

## Apply lifecycle

```
dry-run preview  ->  destructive-lint  ->  [capture pre-image if destructive]
      ->  apply forward (idempotent, IF NOT EXISTS, per-target strategy)
      ->  record ledger (version, checksum, applied-at, success)

rollback:  apply envelope.down (restoring pre-image where destructive)  ->  pop ledger
```

## Migrate x sync: ordered migrations on a masterless fabric (the central open problem)

The crux of the whole feature, and the thing to resolve before any code. The LAN fabric
is masterless, idempotent, last-received-wins -- deliberately *unordered* for rows (no
vector clocks, no CRDTs, no leader). Global ordered schema change is exactly the
coordination the fabric refuses to provide.

Decision 7 proposes the answer for the **masterless-multicast path**: the ledger gates
row-sync on version equality, and a rollout partitions the fabric by version until it
propagates. That works but owes a proof of convergence under concurrent writes during a
rollout.

The genuine fork (open-Q4, blocking):

- **Ride the masterless-multicast path** -- cheap, no coordinator, but you inherit the
  version-gating invariant, the rollout-partition behavior, and the concurrent-writer
  race (open-Q7), and you must prove convergence under concurrent writes.
- **A coordinated apply path** -- a designated applier drives migrations in order
  (deterministic, tractable) at the cost of adding coordination the LAN fabric
  deliberately lacks (the cross-process TCP shape, #90, could carry it).

Sharpest question for the RFC, specifically platform and huttspawn: does
schema-skew-during-rollout match a real multi-node deployment, and is the
partition-until-converged window acceptable? huttspawn's lived D1 apply-mutex experience
is direct evidence it bites.

## Convergent-declarative vs authored-delta (the second open fork)

Raised by mail, and it decides whether surgical rollback even applies to a whole class of
consumers. migrate's precedents (Flyway, goose, pgroll) are **imperative-delta**: an
ordered chain of authored up/down steps. But mail -- a *library* -- ships a **convergent
declarative** artifact: the whole schema as one `CREATE ... IF NOT EXISTS` blob,
deliberately, because a library cannot know each consumer's current schema state
(Atlas-style, the precedent the first draft omitted).

The discriminating question: **does migrate ingest a convergent declarative artifact, or
require an authored delta chain?**

- **Delta-only:** a convergent shipper (mail) must either wrap its whole schema as one
  giant `delta-v1` (which has *no surgical reverse* -- the headline feature does nothing
  for it) or maintain a delta chain that fights "a library can't know consumer state."
  Surgical rollback helps mail *only if mail abandons convergent shipping.*
- **Ingest declarative too:** migrate diffs a target's live schema against a desired
  declarative schema and *generates* the ordered delta (the safe declarative->versioned
  flow from the research -- not a live `db push`). More engine work, but serves app
  authors (delta) and library shippers (declarative) without forcing either to choose.

Orthogonal to the sync fork above, equally blocking for the library-consumer path.
**Leaning:** ingest declarative and *generate* deltas -- the only answer that does not
make mail choose between smugglr and its convergent-shipping contract.

## Security

SQLite has no in-database security model (no `GRANT`, no roles), so smugglr's security
lives at the transport/envelope, not the database. The real surfaces:

- **Peer-supplied SQL** in the multicast path -- table names currently string-matched,
  not validated (the parked `multicast.rs` finding). The fix falls out of this design:
  the manifest's declared `target_schema` + table set is the **allowlist**, and params
  **bind**, never interpolate (http-sql SPEC.md S12: "servers MUST NOT interpolate
  parameters into the SQL string before binding").
- **Destructive DDL** -- covered by the destructive-lint (decision 5).

Not a concern: permission escalation (the supabase `anon`-grant footgun) -- there is no
permission surface in SQLite to mis-grant.

## Open questions

1. **Pre-image granularity.** Full-table snapshot vs affected-rows-only
   (delta-scoped -- cheaper and smugglr-native, but more work to get exactly right).
2. **Reverse location.** Reverse carried *in* the envelope vs derived from the ledger +
   a content-addressed snapshot store.
3. **Manifest field set.** The sketch above needs to become normative.
4. **[BLOCKING -- central] Ordered apply on the masterless fabric.** Masterless-multicast
   (with decision-7 version-gating + rollout partition) vs a coordinated applier. Prove
   convergence under concurrent writes during a rollout before implementation. Plus:
   interaction with `defer_foreign_keys` and per-target apply strategies mid-convergence.
   This is the question the RFC exists to answer (advisor; platform + huttspawn agree it
   is the real design risk).
5. **Author-signature provenance.** AEAD proves un-tampered + key-holder; non-repudiable
   "which agent authored this" needs an asymmetric signature -- a later layer.
6. **WASM crypto gating.** Un-gate `chacha20poly1305` for `wasm32` (decision 2 note).
7. **Concurrent-writer race on txn-less targets (huttspawn).** skip-if-applied covers
   re-run, not two *different* writers racing the ledger insert against D1 (double-apply).
   Needs a lease / mutex / compare-and-swap on the ledger row (huttspawn runs a
   wrangler-apply mutex today).
8. **[BLOCKING -- library path] Convergent-declarative vs authored-delta (mail).** Does
   migrate ingest a whole-schema convergent artifact or require a delta chain? Decides
   whether surgical rollback serves library shippers at all. See the fork section above.
9. **Parser fidelity (mail).** If the manifest diffs/introspects SQL, preserve quoted
   identifiers (mail's `inbox_message."references"`, a reserved word). Test against mail's
   actual `migrationSQL`; a normalizing diff must not mangle quoting.

## Rejected alternatives

- **Cross-engine migration.** ETL territory; dissolves the single-dialect advantage.
- **fence-as-migration-governor.** Turns fence into a kitchen sink; migrate is a
  client capability, fence stays lean transport.
- **Encryption in the http-sql spec.** The executing host must read plaintext, so the
  stamp would be weak (non-host-blind). The envelope rides above http-sql instead.
- **Whole-DB at-rest encryption.** The user's data surface, which smugglr refuses.
- **Hand-written down migrations.** The envelope carries its own reverse.
- **Distributed transactions.** No target supports them across replicas; expand-contract
  + idempotency replaces the need.
- **BLAKE3 manifest.** Filepack is a structural precedent only; reuse smugglr's existing
  SHA-256.
- **Signing-only (no encryption).** Operator chose encryption for the privacy stamp;
  AEAD gives integrity for free, so encryption is the superset.

## Parked: GDPR crypto-shred (future layer)

Explored with platform + mail; **parked, not built** -- migrate ships without it, and it is a
separable additive layer. The idea: combine `@rafters/ledger`'s subject-accounting with
smugglr's crypto substrate to make *replicated SQLite GDPR-erasure-compatible* via
**crypto-shredding** (encrypt subject data under a key; erase = destroy the key; ciphertext
stays everywhere but irrecoverable -- solving the resurrection problem physical delete cannot on
an append-only / replicated / hash-chained fabric). A 5-reviewer consensus hardened it into
locked invariants: opaque `keyref` only (never `subjectId`); the whole API describable without
"PII / subject / GDPR"; KEK/DEK key-wrapping with last-owner-shred; KMS destroy-terminal;
row-level "auth" is confidentiality-at-rest only, never a `grant`/`revoke` verb.

It is gated on **one unsolved crux**: smugglr hashes *plaintext* today (`rowhash.rs:86-124`), so
a plaintext hash of PII survives key destruction (incomplete erasure) -- and hashing ciphertext
collides with content-addressed convergence (AEAD's random nonce breaks determinism). The resume
point is solving **deterministic / keyed content-hashing** for encrypted columns (likely HMAC
under the subject key). Full design + resume anchor: `legion recall --repo smugglr --context
"gdpr crypto-shred parked"` (chain `019f6cd4..019f6ce8`).

## Precedents

- **Filepack** (BLAKE3 checksummed manifest) -- structural model for a hashed manifest.
- **MigrationSafe** (Postgres) -- the destructive-lint precedent.
- **Flyway** -- the versioned-ordered mental model practitioners expect.
- **gh-ost / pgroll** -- zero-downtime / expand-contract lineage.
- **Cloudflare D1 migration tool** -- checkpoints + integrity verification demand.
- **Atlas** -- the declarative / desired-state precedent (the convergent-vs-delta fork).
