# browser-opfs-turso

A real SQLite database in the browser (wa-sqlite + OPFS) syncing to a Turso database. The local-first golden path: the user's data lives on their device and replicates to your Turso edge replica without a relay layer in between.

## Prerequisites

- Node 20+ and pnpm
- A Turso database + auth token. Sign up at <https://turso.tech>; create a database with `turso db create my-app`; mint a token with `turso db tokens create my-app`.
- A modern Chromium-, Firefox-, or Safari-based browser. **wa-sqlite must run inside a Web Worker** for OPFS sync access handles to work in WebKit and Firefox; the example wires this up.

## Setup

```sh
pnpm install
cp .env.example .env
# fill in TURSO_URL and TURSO_TOKEN
pnpm dev
```

Open <http://localhost:5173>.

## Run

The page exposes three buttons:

1. **Add row** -- inserts a row into the local OPFS database.
2. **Sync** -- bidirectional sync with Turso. Local changes push up; remote changes pull down.
3. **Reset** -- wipes the OPFS database (handy for re-running the demo).

Open the same page in another browser window. Each window has its own OPFS database; sync both, and each window will see the other's writes after the next pull.

## What this demonstrates

- The full local-first stack: OPFS-backed SQLite in the browser, content-hashed delta sync, no server in the middle.
- Worker isolation: wa-sqlite + smugglr live in `worker.ts`; the page UI in `main.ts` proxies via postMessage. This is the cross-browser-safe layout because OPFS sync access handles are worker-only in WebKit and Firefox.
- The `createWaSqliteExecutor` adapter wraps wa-sqlite as a `SqlExecutor`. Smugglr is SQLite-runtime-agnostic; swap in better-sqlite3 (Node), sql.js, or the official sqlite-wasm package by writing the same shape.
