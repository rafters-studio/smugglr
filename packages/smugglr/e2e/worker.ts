// Web Worker host: owns wa-sqlite + OPFS + smugglr. Main thread proxies
// RPC calls in via postMessage. SyncAccessHandle is worker-only in WebKit
// and Firefox and is the spec-correct context for OPFS-backed SQLite.

import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
// @ts-expect-error - wa-sqlite ships JS examples without .d.ts
import { OriginPrivateFileSystemVFS } from "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js";

import { Smugglr, createWaSqliteExecutor, setWasm } from "../dist/index.js";
import * as wasm from "../dist/wasm/smugglr_wasm.js";

let sqlite3: any = null;
let db: number | null = null;

async function init(dbPath: string) {
  await (wasm as unknown as { default: () => Promise<unknown> }).default();
  setWasm(wasm as never);
  const module = await SQLiteAsyncESMFactory();
  sqlite3 = SQLite.Factory(module);
  const vfs = new OriginPrivateFileSystemVFS();
  await new Promise((r) => setTimeout(r, 0));
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(
    dbPath,
    SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
    "opfs",
  );
}

async function runSql(sql: string, params: unknown[]) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const exe = createWaSqliteExecutor(sqlite3, db);
  return exe.run(sql, params);
}

async function sync(opts: {
  destUrl: string;
  tables: string[];
  conflict?: "local_wins" | "remote_wins" | "newer_wins" | "uuid_v7_wins";
  direction?: "push" | "pull" | "sync";
}) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: {
      tables: opts.tables,
      conflictResolution: opts.conflict ?? "newer_wins",
    },
  });
  const dir = opts.direction ?? "sync";
  const result = dir === "push" ? await s.push()
    : dir === "pull" ? await s.pull()
    : await s.sync();
  s.dispose();
  return result;
}

async function reset() {
  if (sqlite3 && db !== null) {
    sqlite3.close(db);
    db = null;
  }
  const root = await navigator.storage.getDirectory();
  for await (const entry of (root as any).values()) {
    await root.removeEntry(entry.name, { recursive: true });
  }
}

interface RpcCall {
  id: number;
  op: "init" | "runSql" | "sync" | "reset";
  args: unknown[];
}

self.addEventListener("message", async (ev: MessageEvent<RpcCall>) => {
  const { id, op, args } = ev.data;
  try {
    let result: unknown;
    switch (op) {
      case "init": result = await init(args[0] as string); break;
      case "runSql": result = await runSql(args[0] as string, (args[1] as unknown[]) ?? []); break;
      case "sync": result = await sync(args[0] as Parameters<typeof sync>[0]); break;
      case "reset": result = await reset(); break;
    }
    (self as unknown as Worker).postMessage({ id, ok: true, result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    (self as unknown as Worker).postMessage({ id, ok: false, error: message });
  }
});
