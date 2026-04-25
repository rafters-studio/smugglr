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

```ts
import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
import { OPFSCoopSyncVFS } from "wa-sqlite/src/examples/OPFSCoopSyncVFS.js";
import { Smugglr, createWaSqliteExecutor } from "smugglr";

// One-time wa-sqlite + OPFS setup
const module = await SQLiteAsyncESMFactory();
const sqlite3 = SQLite.Factory(module);
const vfs = await OPFSCoopSyncVFS.create("opfs", module);
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

## License

MIT
