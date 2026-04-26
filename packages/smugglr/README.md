# smugglr

Content-hashed delta sync for SQLite, in the browser.

The smugglr sync engine compiled to WebAssembly with a small typed wrapper. Push, pull, or bidirectionally sync between any two HTTP-SQL endpoints (Turso, rqlite, Cloudflare D1, custom) directly from the browser or Node. Only rows that actually changed are sent.

Homepage: [smugglr.dev](https://smugglr.dev)

## Install

```sh
pnpm add smugglr
```

## Usage

```ts
import { Smugglr } from "smugglr";

const s = await Smugglr.init({
  source: { url: "https://my-db.turso.io", authToken: "tok", profile: "turso" },
  dest:   { url: "https://api.cloudflare.com/...", authToken: "cf", profile: "d1" },
  sync:   { tables: ["users", "posts"], conflictResolution: "local_wins" },
});

await s.sync();          // bidirectional
await s.push();          // source -> dest
await s.pull();          // dest -> source
const plan = await s.diff(); // dry-run plan, no writes

s.dispose();
```

`using` is supported:

```ts
using s = await Smugglr.init(config);
await s.sync();
// freed at scope exit
```

## Local SQLite (browser, OPFS)

Sync a real SQLite database in the browser (via [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) on OPFS) to any HTTP SQL backend:

> OPFS sync access handles are worker-only in WebKit and Firefox. Run wa-sqlite + smugglr inside a Web Worker for cross-browser compatibility (Chromium allows main-thread use, but worker context is the spec-portable choice).

```ts
import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
import { OriginPrivateFileSystemVFS } from "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js";
import { Smugglr, createWaSqliteExecutor } from "smugglr";

// One-time wa-sqlite + OPFS setup (run inside a Web Worker)
const module = await SQLiteAsyncESMFactory();
const sqlite3 = SQLite.Factory(module);
const vfs = new OriginPrivateFileSystemVFS();
sqlite3.vfs_register(vfs, true);
const db = await sqlite3.open_v2(
  "app.db",
  SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
  "opfs",
);

// Local SQLite <-> Turso, content-hashed delta
const s = await Smugglr.init({
  source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
  dest:   { url: "https://my-db.turso.io", authToken: "...", profile: "turso" },
  sync:   { tables: ["users", "posts"], conflictResolution: "newer_wins" },
});

await s.sync();
```

`wa-sqlite` is the first shipped runtime, but the local executor contract is generic. Any object satisfying `SqlExecutor` (`run(sql, params): Promise<{columns, rows}>`) plugs in -- better-sqlite3 in Node, the official sqlite-wasm package, sql.js, or your own.

## Custom WASM loading

If your bundler resolves `.wasm` imports differently or you serve the binary from a CDN:

```ts
import { Smugglr, setWasm } from "smugglr";
import * as wasm from "smugglr/wasm";

await wasm.default("https://cdn.example.com/smugglr_wasm_bg.wasm");
setWasm(wasm);

const s = await Smugglr.init(config);
```

## Reactive events

Subscribe to `table-changed` to react to sync writes without polling. Fires once per affected table after `pull` or `sync` completes the local write; `push` and `diff` never emit.

```ts
const unsub = s.on("table-changed", (e) => {
  // e.table          -- which table was modified
  // e.changedPks     -- primary keys of inserted/updated rows
  // e.removedPks     -- reserved; always [] until delete propagation lands
  // e.source         -- "pull" | "sync"
  console.log(`${e.table} +${e.changedPks.length} via ${e.source}`);
});

await s.sync();
unsub(); // remove this listener
```

This is the primitive the framework binding plugins (`@smugglr/zustand`, `@smugglr/nanostores`, etc.) are built on.

## GDPR / right-to-erasure

`eraseLocal()` empties every configured sync table on the local SQLite database and clears smugglr's in-memory caches. Schema and any non-synced tables are untouched; the dest endpoint is not contacted (server-side erasure is the app's concern, typically via its auth/account system).

```ts
const result = await s.eraseLocal();
// { erasedTables: ["users", "posts"] }
```

## Auth rotation and dest swapping

`updateAuth(token)` rotates the dest auth token without re-initializing the WASM module or losing the metadata cache. `updateDest(dest)` replaces the entire dest endpoint -- URL, profile, token -- and clears only the dest cache (source cache survives).

```ts
// Token rotation
s.updateAuth(newToken);
await s.sync(); // uses newToken

// Anonymous -> account upgrade
s.updateDest({
  url: "https://api.acme.com/sync",
  authToken: accountToken,
  profile: "generic",
});
await s.sync(); // first sync against the new dest re-scans then pushes
```

> **Do not call while a sync future is pending.** Await any in-flight push/pull/sync first; the implementation borrows the dest across awaits and a concurrent mutation will panic.

## Bundle size

| Module                                         | Compressed (gzip) |
| ---------------------------------------------- | ----------------- |
| `smugglr` (TS wrapper)                         | ~2 KB             |
| `smugglr/wasm` glue (`smugglr_wasm.js`)        | ~8 KB             |
| `smugglr_wasm_bg.wasm` (sync engine)           | ~110 KB           |
| `wa-sqlite-async.wasm` (peer, OPFS path only)  | ~410 KB           |
| `wa-sqlite-async.mjs` glue (peer)              | ~15 KB            |

`wa-sqlite` is only required for the local OPFS path. HTTP-only sync (e.g. Turso → D1) ships without it. Smugglr declares `wa-sqlite` as a peer dependency rather than bundling it, so consumers control the version and the bundler can tree-shake it out when only HTTP endpoints are configured.

## End-to-end tests

A Playwright suite under `e2e/` exercises the full local-OPFS path against a mocked HTTP target. Run with:

```sh
pnpm install
pnpm test:e2e:install   # one-time: download chromium
pnpm test:e2e
```

## License

MIT
