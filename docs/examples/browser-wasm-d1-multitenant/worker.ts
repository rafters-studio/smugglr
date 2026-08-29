// Web Worker: owns wa-sqlite + OPFS + smugglr. Each tenant gets its own
// OPFS database (filename includes tenant id) so two tabs cannot collide.
//
// The browser uses profile: "d1" pointed at the Cloudflare Worker URL. The
// Worker mimics D1's HTTP shape exactly, so nothing on the smugglr side knows
// or cares that a guard sits in between.

import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
import { OriginPrivateFileSystemVFS } from "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js";
import {
  Smugglr,
  createWaSqliteExecutor,
  type SqlExecutor,
  type TableChangedEvent,
} from "smugglr";

type SQLiteAPI = ReturnType<typeof SQLite.Factory>;

interface Request {
  id: number;
  op: "init" | "addRow" | "listRows" | "sync" | "reset";
  args: unknown[];
}

// Replies carry the request id. Events carry no id; the page logs them.
type Reply =
  | { id: number; ok: true; result: unknown }
  | { id: number; ok: false; error: string }
  | { event: "table-changed"; detail: TableChangedEvent };

let sqlite3: SQLiteAPI | null = null;
let db: number | null = null;
let executor: SqlExecutor | null = null;
let smugglr: Smugglr | null = null;
let tenantId: string | null = null;

function post(reply: Reply) {
  (self as unknown as Worker).postMessage(reply);
}

async function init(tenant: string, tenantToken: string, guardUrl: string) {
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

  executor = createWaSqliteExecutor(sqlite3, db);
  // tenant_id is part of the schema so smugglr's row-level push carries it
  // upstream and the guard Worker can validate it against the auth token.
  await executor.run(
    "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, body TEXT, updated_at TEXT)",
    [],
  );

  smugglr = await Smugglr.init({
    source: { type: "local", executor },
    dest: { url: guardUrl, authToken: tenantToken, profile: "d1" },
    sync: { tables: ["notes"], conflictResolution: "newer_wins" },
  });

  // Fires once per table after pull or sync writes locally; push never emits.
  smugglr.on("table-changed", (detail) => post({ event: "table-changed", detail }));
}

async function addRow(id: string, updatedAt: string) {
  if (!executor || !tenantId) throw new Error("init() first");
  await executor.run(
    "INSERT INTO notes (id, tenant_id, body, updated_at) VALUES (?, ?, ?, ?)",
    [id, tenantId, `note from ${tenantId} at ${updatedAt}`, updatedAt],
  );
}

async function listRows() {
  if (!executor) throw new Error("init() first");
  const result = await executor.run(
    "SELECT id, tenant_id, body, updated_at FROM notes ORDER BY updated_at DESC",
    [],
  );
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
  executor = null;
  if (sqlite3 && db !== null) {
    await sqlite3.close(db);
    db = null;
  }
  // Only this tenant's file and its sidecars: other tenants' tabs share the
  // origin, and their databases are theirs.
  if (tenantId === null) return;
  const own = `tenant-${tenantId}.db`;
  const root = await navigator.storage.getDirectory();
  for await (const entry of root.values()) {
    if (entry.name === own || entry.name.startsWith(`${own}-`)) {
      await root.removeEntry(entry.name, { recursive: true });
    }
  }
}

self.addEventListener("message", async (ev: MessageEvent<Request>) => {
  const { id, op, args } = ev.data;
  try {
    let result: unknown;
    switch (op) {
      case "init": result = await init(args[0] as string, args[1] as string, args[2] as string); break;
      case "addRow": result = await addRow(args[0] as string, args[1] as string); break;
      case "listRows": result = await listRows(); break;
      case "sync": result = await sync(); break;
      case "reset": result = await reset(); break;
      default: throw new Error(`unknown op: ${String(op)}`);
    }
    post({ id, ok: true, result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    post({ id, ok: false, error: message });
  }
});
