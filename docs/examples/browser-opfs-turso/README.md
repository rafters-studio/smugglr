# browser-opfs-turso

A real SQLite database in the browser (wa-sqlite on OPFS) syncing to a Turso database. The user's data lives on their device and replicates to Turso with no relay in between. The example builds with `pnpm build` against smugglr 0.5.0.

## Prerequisites

| Requirement | Detail |
| ----------- | ------ |
| Node 20+ and pnpm | `pnpm build` runs `tsc --noEmit` and then `vite build`. |
| A Turso database URL | `turso db create my-app`, then `turso db show my-app --url`, which prints a `libsql://` URL; use that host with `https://`. Goes in `VITE_TURSO_URL`. |
| A Turso auth token | `turso db tokens create my-app`. Goes in `VITE_TURSO_TOKEN`. |
| A browser with OPFS | Current Chromium, Firefox, or Safari. wa-sqlite runs inside a Web Worker because OPFS sync access handles are worker-only in WebKit and Firefox; `worker.ts` wires this up. |

Both Vite variables are read at build time and shipped to the browser. The token is visible to anyone who loads the page, so use a token scoped to a throwaway database.

## Setup

```sh
pnpm install
cp .env.example .env
# fill in VITE_TURSO_URL and VITE_TURSO_TOKEN
pnpm dev
```

Open <http://localhost:5173>. `pnpm build` produces a static bundle under `dist/`.

## Run

The page exposes three buttons and a log pane. Each action prepends one timestamped line to the pane.

| Button | Effect |
| ------ | ------ |
| Add row | Inserts one row into the local OPFS database and logs its id. |
| Sync | Bidirectional sync with Turso. Local changes push up; remote changes pull down. Logs the `SyncResult` that `sync()` returned: `command`, `status`, and a `tables` entry per table with `rowsPushed` and `rowsPulled`. |
| Reset | Disposes the client, closes the database, and wipes OPFS. Reload the page to start again. |

When a pull writes rows locally, the worker forwards the `table-changed` event and the pane logs it with the table name and the primary keys that changed. A push never emits one.

Open the same page in a second window. Each window has its own OPFS database; sync both, and each sees the other's rows after its next pull.

## What this demonstrates

The full local-first stack: OPFS-backed SQLite in the browser, content-hashed delta sync, no server in the middle.

Worker isolation: wa-sqlite and smugglr live in `worker.ts`; the page UI in `main.ts` proxies calls via `postMessage`. This is the cross-browser-safe layout because OPFS sync access handles are worker-only in WebKit and Firefox.

The `createWaSqliteExecutor` adapter wraps wa-sqlite as a `SqlExecutor`. Smugglr is SQLite-runtime-agnostic; better-sqlite3 (Node), sql.js, or the official sqlite-wasm package plug in by satisfying the same shape.

The `table-changed` event, subscribed with `on("table-changed", handler)`, fires once per affected table after `pull` or `sync` writes locally. It is the primitive the framework bindings (`@smugglr/zustand`, `@smugglr/nanostores`) are built on.

## Limits

Deletes do not replicate. A row deleted locally stays in Turso and returns on the next pull; model deletion as a `deleted_at` column that rides the upsert path.

This example was built but not run against a live Turso database; nobody maintaining it holds Turso credentials. The build is verified, the round trip is not.
