// Web Worker: owns wa-sqlite + OPFS + smugglr. Each tenant gets its own
// OPFS database (filename includes tenant id) so two tabs cannot collide.
//
// The browser uses profile: "d1" pointed at the Cloudflare Worker URL. The
// Worker mimics D1's HTTP shape exactly, so nothing on the smugglr side knows
// or cares that a fence sits in between.

import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
// @ts-expect-error - wa-sqlite ships JS examples without .d.ts
import { OriginPrivateFileSystemVFS } from "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js";
import { Smugglr, createWaSqliteExecutor } from "smugglr";

let sqlite3: any = null;
let db: number | null = null;
let smugglr: Smugglr | null = null;
let tenantId: string | null = null;

async function init(tenant: string, tenantToken: string, fenceUrl: string) {
  tenantId = tenant;

  const module = await SQLiteAsyncESMFactory();
  sqlite3 = SQLite.Factory(module);
  const vfs = new OriginPrivateFileSystemVFS();
  await new Promise((r) => setTimeout(r, 0));
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(
    `tenant-${tenant}.db`,
    SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
    "opfs",
  );

  const exe = createWaSqliteExecutor(sqlite3, db);
  // tenant_id is part of the schema so smugglr's row-level push carries it
  // upstream and the fence Worker can validate it against the auth token.
  await exe.run(
    "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, body TEXT, updated_at TEXT)",
    [],
  );

  smugglr = await Smugglr.init({
    source: { type: "local", executor: exe },
    dest: { url: fenceUrl, authToken: tenantToken, profile: "d1" },
    sync: { tables: ["notes"], conflictResolution: "newer_wins" },
  });
}

async function addRow(id: string, updatedAt: string) {
  if (!sqlite3 || db === null || !tenantId) throw new Error("init() first");
  const exe = createWaSqliteExecutor(sqlite3, db);
  await exe.run(
    "INSERT INTO notes (id, tenant_id, body, updated_at) VALUES (?, ?, ?, ?)",
    [id, tenantId, `note from ${tenantId} at ${updatedAt}`, updatedAt],
  );
}

async function listRows() {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const exe = createWaSqliteExecutor(sqlite3, db);
  const result = await exe.run("SELECT id, tenant_id, body, updated_at FROM notes ORDER BY updated_at DESC", []);
  return result.rows;
}

async function sync() {
  if (!smugglr) throw new Error("init() first");
  return smugglr.sync();
}

async function reset() {
  if (smugglr) {
    smugglr.dispose();
    smugglr = null;
  }
  if (sqlite3 && db !== null) {
    sqlite3.close(db);
    db = null;
  }
  const root = await navigator.storage.getDirectory();
  for await (const entry of (root as any).values()) {
    await root.removeEntry(entry.name, { recursive: true });
  }
}

self.addEventListener("message", async (ev: MessageEvent<{ id: number; op: string; args: unknown[] }>) => {
  const { id, op, args } = ev.data;
  try {
    let result: unknown;
    switch (op) {
      case "init": result = await init(args[0] as string, args[1] as string, args[2] as string); break;
      case "addRow": result = await addRow(args[0] as string, args[1] as string); break;
      case "listRows": result = await listRows(); break;
      case "sync": result = await sync(); break;
      case "reset": result = await reset(); break;
      default: throw new Error(`unknown op: ${op}`);
    }
    (self as unknown as Worker).postMessage({ id, ok: true, result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    (self as unknown as Worker).postMessage({ id, ok: false, error: message });
  }
});
