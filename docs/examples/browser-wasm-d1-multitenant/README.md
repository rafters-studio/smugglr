# browser-wasm-d1-multitenant

Many browsers, each holding its own full SQLite database, all syncing into one shared D1. Rows in D1 are partitioned by `tenant_id`. A small Cloudflare Worker is the guard: it authenticates each request, scopes reads to the caller's tenant, and rejects writes carrying anyone else's id. The browser app builds with `pnpm build` against smugglr 0.5.0, and the Worker typechecks with `tsc`.

## The model

```
+----------------+         +----------------+         +-------------------+         +------+
| browser tab    |         | browser tab    |         | Cloudflare Worker |         |  D1  |
| ?tenant=alice  |  HTTPS  | ?tenant=bob    |  HTTPS  |   (tenant guard)  |  bind   | one  |
| wa-sqlite+OPFS |-------> | wa-sqlite+OPFS |-------> |  Bearer -> tenant |-------> | DB   |
| tenant-alice.db|         | tenant-bob.db  |         |  scope + validate |         |      |
+----------------+         +----------------+         +-------------------+         +------+
        ^                                                                              |
        |                              tenant-scoped rows only                          |
        +------------------------------------------------------------------------------+
```

| Layer | Role |
| ----- | ---- |
| Browser | Runs `smugglr` over `wa-sqlite` on OPFS. Each tenant's local database is its own OPFS file (`tenant-<id>.db`), so two tabs on one origin do not trample one another. |
| Worker | Speaks D1's HTTP shape, so the browser uses `profile: "d1"` unchanged and points at the Worker URL. The Worker does the tenancy work the smugglr client cannot do on its own. |
| D1 | Holds every tenant's rows in one table. The `tenant_id` column is the partition key. |

## Why the Worker exists

`smugglr-core` does not ship a per-tenant `WHERE` filter on `pull`. Without something in the middle, every browser would pull every tenant's rows out of the shared D1. The Worker is what makes shared-D1 multi-tenancy real: it authenticates the request, scopes reads to the caller's tenant, and validates that writes carry the matching id.

## Prerequisites

| Requirement | Detail |
| ----------- | ------ |
| Node 20+ and pnpm | Both the browser app and `tenant-worker/` install with `pnpm install`. |
| A Cloudflare account with Workers and D1 | `pnpm dlx wrangler login` authenticates wrangler against it. Deploying the Worker and creating the D1 database both need it. |
| A D1 database id | Printed by `wrangler d1 create`; pasted into `tenant-worker/wrangler.toml`. |
| The Worker URL | Printed by `wrangler deploy`; goes in `VITE_GUARD_URL`. |
| One token per tenant | Demo values live in `tenant-worker/wrangler.toml` under `[vars]` and in `.env` as `VITE_TENANT_TOKEN_<TENANT>`. The two must match. |
| A browser with OPFS | Current Chromium, Firefox, or Safari. OPFS sync access handles are worker-only outside Chromium; `worker.ts` wires that up. |

The tenant tokens are read at build time and shipped to the browser, which is fine for a demo and wrong for production; see Limits.

## Setup

### 1. Create the D1 database and apply the schema

```sh
cd tenant-worker
pnpm dlx wrangler d1 create smugglr_demo
```

Wrangler prints a `database_id`. Paste it into `tenant-worker/wrangler.toml` in place of `REPLACE_WITH_YOUR_D1_DATABASE_ID`.

```sh
pnpm install
pnpm db:schema
```

`db:schema` runs `wrangler d1 execute smugglr_demo --file=../schema.sql --remote`.

pnpm 11 writes a `pnpm-workspace.yaml` into the example root when it installs the browser app (step 3). After that, a `pnpm install` inside `tenant-worker/` reports "Already up to date" and installs nothing; run it as `pnpm install --ignore-workspace` instead.

### 2. Deploy the guard Worker

```sh
pnpm deploy
```

Wrangler prints a `*.workers.dev` URL. Note it.

### 3. Configure the browser app

From the example root:

```sh
pnpm install
cp .env.example .env
```

Edit `.env`:

```
VITE_GUARD_URL=https://smugglr-tenant-guard.<your-subdomain>.workers.dev
VITE_TENANT_TOKEN_ALICE=alice-dev-token
VITE_TENANT_TOKEN_BOB=bob-dev-token
```

The token values must match the `[vars]` in `tenant-worker/wrangler.toml`. A real app generates proper tokens and ships them through its auth flow; this example uses a hardcoded map so the moving parts stay visible.

### 4. Run

```sh
pnpm dev
```

Open two windows, <http://localhost:5173/?tenant=alice> and <http://localhost:5173/?tenant=bob>. The heading names the tenant. Each page exposes four buttons and a log pane; each action prepends one timestamped line.

| Button | Effect |
| ------ | ------ |
| Add note | Inserts one row tagged with this tab's `tenant_id` into its OPFS database. |
| Sync | Bidirectional sync through the guard. Logs the `SyncResult` that `sync()` returned. |
| List local rows | Logs every row in this tab's local database. |
| Reset local | Disposes the client, closes the database, and wipes OPFS. Reload to start again. |

In each window, click Add note a few times, then Sync. List local rows shows only that tenant's rows. Switch to the other window, click Sync, then List local rows: it still holds only its own tenant's rows, although D1 now holds both sets. When a pull writes rows locally, the pane also logs the `table-changed` event with the table name and the changed primary keys.

## What this demonstrates

Tenant partitioning at the table level. Every row carries `tenant_id`; there is no database-per-tenant overhead.

The guard does what the client cannot. The browser is trusted enough to tag its own rows; the Worker is what stops a bad client from inserting rows tagged with someone else's id.

The D1 profile is a contract. The Worker mimics D1's HTTP shape so the browser code is identical to a direct-to-D1 setup. Swap `VITE_GUARD_URL` for a real D1 endpoint and the client does not notice.

Per-tenant OPFS isolation. Two tabs on the same origin do not collide because the OPFS filename embeds the tenant id.

## What the Worker does

`tenant-worker/src/index.ts` resolves `Bearer <token>` to a `tenant_id` through a hardcoded map, then passes the request's SQL through `enforce()`, which handles every statement the smugglr d1 adapter (`crates/smugglr-wasm/src/fetch_adapter.rs`) emits against the destination.

| Statement | Treatment |
| --------- | --------- |
| `SELECT name FROM sqlite_master ...` | Passed through. Table discovery reads no tenant rows. |
| `PRAGMA table_info('notes')` | Passed through. Column and primary-key discovery reads no tenant rows. |
| `SELECT *, CAST("id" AS TEXT) AS __pk FROM "notes"` and the `WHERE ... IN (?, ...)` row fetch | Wrapped as a subquery with `WHERE tenant_id = ?` appended and the authenticated tenant bound last. The subquery preserves the original projection, so no parsing is needed. |
| `INSERT OR REPLACE INTO "notes" (...) VALUES (...), (...)` | The column list is parsed, `tenant_id` located, and every row in the batched `VALUES` checked against the authenticated tenant. A mismatch rejects the whole batch. |
| Anything else | Rejected. |

The rewriter is naive on purpose; it exists to make the pattern visible. A production Worker would use prepared-statement enforcement, JWT claims for the tenant, and a real allowlist instead of a hardcoded token map.

## Other clients

The guard sees only HTTP, so any smugglr client that speaks the `d1` profile can point at it: the CLI through the http-sql plugin, a Node process using the npm package, or a Rust program on `smugglr-core`. Each becomes another tenant by presenting its own Bearer token and carrying `tenant_id` in every inserted row, the same way the browser does.

## Limits

Push uses `INSERT OR REPLACE`. The 0.5.0 changelog entry for #324 moved the native apply path to `INSERT ... ON CONFLICT(pk) DO UPDATE` for rows that omit a column, but that path is the directional `pull`, stash, and multicast writes into a local database. This example's write path is the wasm fetch adapter pushing to D1, which still emits `INSERT OR REPLACE`; SQLite executes that as DELETE+INSERT, so a pushed row that omits a column resets it at D1. Keep every synced column in the local schema and the issue does not arise.

Deletes do not replicate. Per #311, deletion propagates by no path in 0.5.0: a row deleted locally stays in D1 and returns on the next pull, and the guard rejects `DELETE` outright. Model deletion as a `deleted_at` column, which rides the upsert path; when tombstone propagation lands, the guard will need to gate `DELETE` the way it gates `INSERT`.

The token map lives in plaintext `wrangler.toml [vars]`, and the tokens are compiled into the browser bundle. Use Workers secrets and a real auth flow in production.

The Worker has no rate limiting, audit log, or per-tenant quota. Add them at the Worker layer if you need them.

This example was built but not run against a live Worker and D1; nobody maintaining it holds Cloudflare credentials. The build and the `enforce()` rewrites are verified, the round trip is not.
