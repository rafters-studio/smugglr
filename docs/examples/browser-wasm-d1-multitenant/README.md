# browser-wasm-d1-multitenant

Many browsers, each holding their own full SQLite database, all syncing into one shared D1. Rows in D1 are partitioned by `tenant_id`. A small Cloudflare Worker is the fence: it authenticates each request, scopes reads to the caller's tenant, and rejects writes carrying anyone else's id.

## The model

```
+----------------+         +----------------+         +-------------------+         +------+
| browser tab    |         | browser tab    |         | Cloudflare Worker |         |  D1  |
| ?tenant=alice  |  HTTPS  | ?tenant=bob    |  HTTPS  |   (tenant fence)  |  bind   | one  |
| wa-sqlite+OPFS |-------> | wa-sqlite+OPFS |-------> |  Bearer -> tenant |-------> | DB   |
| tenant-alice.db|         | tenant-bob.db  |         |  scope + validate |         |      |
+----------------+         +----------------+         +-------------------+         +------+
        ^                                                                              |
        |                              tenant-scoped rows only                          |
        +------------------------------------------------------------------------------+
```

- **Browser** runs `smugglr` over `wa-sqlite` on OPFS. Each tenant's local database is its own OPFS file (`tenant-<id>.db`), so two tabs don't trample one another.
- **Worker** speaks D1's HTTP shape exactly, so the browser uses `profile: "d1"` unchanged and points at the Worker URL. The Worker does the tenancy work the smugglr client cannot do on its own.
- **D1** holds every tenant's rows in one table. The `tenant_id` column is the partition key.

## Why the Worker exists

`smugglr-core` does not ship a per-tenant `WHERE` filter on `pull`. Without something in the middle, every browser would pull every tenant's rows out of the shared D1. The Worker is what makes shared-D1 multi-tenancy real -- it authenticates the request, scopes reads to the caller's tenant, and validates that writes carry the matching id.

## Prerequisites

- Node 20+, pnpm
- A Cloudflare account with D1 and Workers enabled
- `wrangler` available (`pnpm dlx wrangler --help`)
- A modern Chromium/Firefox/Safari (OPFS sync access handles are worker-only outside Chromium; the example already wires that up)

## Setup

### 1. Create the D1 database and apply the schema

```sh
cd tenant-worker
pnpm dlx wrangler d1 create smugglr_demo
```

Wrangler prints a `database_id`. Paste it into `tenant-worker/wrangler.toml` (replace `REPLACE_WITH_YOUR_D1_DATABASE_ID`).

```sh
pnpm install
pnpm db:schema
```

`db:schema` is just `wrangler d1 execute smugglr_demo --file=../schema.sql --remote`.

### 2. Deploy the fence Worker

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
VITE_FENCE_URL=https://smugglr-tenant-fence.<your-subdomain>.workers.dev
VITE_TENANT_TOKEN_ALICE=alice-dev-token
VITE_TENANT_TOKEN_BOB=bob-dev-token
```

The token values must match the `[vars]` in `tenant-worker/wrangler.toml`. For real apps, generate proper tokens and ship them through your auth flow; this example uses a hardcoded map so the moving parts stay visible.

### 4. Run

```sh
pnpm dev
```

Open two windows:

- <http://localhost:5173/?tenant=alice>
- <http://localhost:5173/?tenant=bob>

In each: click **Add note** a few times, then **Sync**. Click **List local rows** to confirm each browser only holds its own tenant's rows. Switch to the other tab, click **Sync**, then **List local rows** -- you should still only see that tenant's rows, even though D1 now holds both sets.

## What this demonstrates

- **Tenant partitioning at the table level.** Every row carries `tenant_id`. There is no separate-database-per-tenant overhead.
- **The fence does what the client cannot.** The browser is trusted enough to tag its own rows; the Worker is what stops a bad client from inserting rows tagged with someone else's id.
- **The D1 profile is a contract.** The Worker mimics D1's HTTP shape so the browser code is identical to a direct-to-D1 setup. Swap `VITE_FENCE_URL` for a real D1 endpoint and the client doesn't notice.
- **Per-tenant OPFS isolation.** Two tabs on the same origin do not collide because the OPFS filename embeds the tenant id.

## What the Worker actually does

`tenant-worker/src/index.ts` is ~120 lines. The interesting half:

- Resolves `Bearer <token>` to a `tenant_id` via a hardcoded map.
- For `SELECT`s, wraps the query as a subquery and appends `WHERE tenant_id = ?` bound to the authenticated tenant. This works for every shape smugglr's d1 adapter emits because the subquery preserves the original projection.
- For `INSERT OR REPLACE`, parses the column list, finds `tenant_id`, and verifies every row in the batched VALUES matches the authenticated tenant.
- Passes through `SELECT 1` and `sqlite_master` queries (smugglr's connection ping and table discovery).
- Rejects every other statement shape.

The rewriter is naive on purpose -- it's there to make the pattern visible. A production version of this Worker would use prepared-statement enforcement, JWT claims for the tenant, and a real allowlist instead of a hardcoded token map.

## Lesson 2 (sketch): same pattern, Rust client

The same fence Worker accepts any `smugglr` client speaking the `d1` profile. A Rust client built on `smugglr-core` (e.g. a desktop companion app, a migration script, or a server-side admin tool) becomes another tenant by:

1. Resolving a tenant token through whatever auth flow it uses.
2. Building a target with `Profile::d1()`, pointing at the Worker URL, with the token in the `Bearer` header.
3. Carrying `tenant_id` in every inserted row, the same way the browser does.

Code for this lesson lives in `rust-custom-datasource/` already as a starting point; the only delta is the dest configuration.

## Limits to call out

- This example does not handle deletes. `smugglr` push uses `INSERT OR REPLACE`; once row deletion lands, the fence will need to gate `DELETE` the same way it gates `INSERT`.
- The token map lives in plaintext `wrangler.toml [vars]`. Use Workers secrets in production.
- The Worker has no rate limiting, audit log, or per-tenant quota. Add them at the Worker layer if you need them.
