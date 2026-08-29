# browser-idb-turso

The same demo as [browser-opfs-turso](../browser-opfs-turso/), backed by IndexedDB (`IDBBatchAtomicVFS`) instead of OPFS. The smugglr API is identical; the only thing that changes is which wa-sqlite VFS gets registered. The example builds with `pnpm build` against smugglr 0.5.0.

## When to pick IndexedDB

The smugglr package README states the constraint on the OPFS path: OPFS sync access handles are worker-only in WebKit and Firefox, so wa-sqlite must run inside a Web Worker there (Chromium also allows main-thread use). The IndexedDB VFS needs no sync access handle, which is why it is the fallback for three situations.

| Situation | Why IndexedDB |
| --------- | ------------- |
| A browser without OPFS | IndexedDB has shipped in every browser for over a decade. |
| An embedded webview that does not expose OPFS | The VFS depends only on IndexedDB, which webviews expose. |
| Code that must run on the main thread | The VFS has no worker-only dependency; the example still uses a worker for parity with the OPFS variant. |

Writes through IndexedDB are slower than through an OPFS access handle: there is no direct file handle, and every write batches into a transaction. Pick OPFS where you can; pick IndexedDB where you must.

## Prerequisites

Same as [browser-opfs-turso](../browser-opfs-turso/): Node 20+ with pnpm, a Turso database URL in `VITE_TURSO_URL`, and a Turso auth token in `VITE_TURSO_TOKEN`. Both are shipped to the browser at build time, so use a token scoped to a throwaway database.

## Setup

```sh
pnpm install
cp .env.example .env
# fill in VITE_TURSO_URL and VITE_TURSO_TOKEN
pnpm dev
```

Open <http://localhost:5173>. `pnpm build` produces a static bundle under `dist/`.

## Run

The buttons behave as in the OPFS demo: Add row inserts locally, Sync runs a bidirectional sync and logs the returned `SyncResult`, and Reset disposes the client and deletes the IndexedDB database. The log pane also prints each `table-changed` event a pull produces.

## What this demonstrates

VFS choice is the only consumer-visible difference between OPFS and IndexedDB persistence in wa-sqlite. Smugglr cares only about the `SqlExecutor` shape, never the underlying VFS.

`IDBBatchAtomicVFS` is constructed with the IndexedDB database name, and that name doubles as the VFS name passed to `open_v2`. Reset deletes that database by name.

## Limits

Deletes do not replicate. A row deleted locally stays in Turso and returns on the next pull; model deletion as a `deleted_at` column that rides the upsert path.

This example was built but not run against a live Turso database; nobody maintaining it holds Turso credentials. The build is verified, the round trip is not.
