# browser-idb-turso

Same demo as [browser-opfs-turso](../browser-opfs-turso/), but backed by IndexedDB (`IDBBatchAtomicVFS`) instead of OPFS. Use this when:

- You need to support older Safari versions where OPFS support is incomplete or buggy.
- You're shipping inside an embedded webview where OPFS isn't reliably exposed.
- You want a single VFS that works across main thread *and* worker contexts without conditional branching.

The smugglr API is identical to the OPFS variant. The only thing that changes is which wa-sqlite VFS gets registered.

## Prerequisites

Same as [browser-opfs-turso](../browser-opfs-turso/).

## Setup

```sh
pnpm install
cp .env.example .env
# fill in TURSO_URL and TURSO_TOKEN
pnpm dev
```

## Run

Open <http://localhost:5173>. Buttons behave identically to the OPFS demo.

## What this demonstrates

- VFS choice is the only consumer-visible difference between OPFS and IndexedDB persistence in wa-sqlite. Smugglr cares only about the `SqlExecutor` shape, not the underlying VFS.
- IndexedDB-backed SQLite has slower writes than OPFS (no direct file handle, batched in transactions) but works in every browser, every context. Pick OPFS where you can; pick IDB where you must.
- Because wa-sqlite's IDB VFS does not need `SyncAccessHandle`, you can run this on the main thread without a worker. The example still uses a worker for parity with the OPFS variant.
