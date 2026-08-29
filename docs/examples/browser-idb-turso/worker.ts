// Worker: same as browser-opfs-turso/worker.ts but registers
// IDBBatchAtomicVFS instead of OriginPrivateFileSystemVFS. Compare against
// the OPFS variant to see what (little) changes between VFS choices.

import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
import { IDBBatchAtomicVFS } from "wa-sqlite/src/examples/IDBBatchAtomicVFS.js";
import {
  Smugglr,
  createWaSqliteExecutor,
  type SqlExecutor,
  type TableChangedEvent,
} from "smugglr";

type SQLiteAPI = ReturnType<typeof SQLite.Factory>;

// The IndexedDB database name doubles as the VFS name passed to open_v2.
const IDB_NAME = "smugglr-demo";

interface Request {
  id: number;
  op: "init" | "addRow" | "sync" | "reset";
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

function post(reply: Reply) {
  (self as unknown as Worker).postMessage(reply);
}

async function init(tursoUrl: string, tursoToken: string) {
  const module = await SQLiteAsyncESMFactory();
  sqlite3 = SQLite.Factory(module);
  // The constructor takes (idbDatabaseName, options); the IndexedDB database
  // is created on first use.
  const vfs = new IDBBatchAtomicVFS(IDB_NAME);
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(
    "demo.db",
    SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
    IDB_NAME,
  );

  executor = createWaSqliteExecutor(sqlite3, db);
  await executor.run(
    "CREATE TABLE IF NOT EXISTS notes (id TEXT PRIMARY KEY, body TEXT, updated_at TEXT)",
    [],
  );

  smugglr = await Smugglr.init({
    source: { type: "local", executor },
    dest: { url: tursoUrl, authToken: tursoToken, profile: "turso" },
    sync: { tables: ["notes"], conflictResolution: "newer_wins" },
  });

  // Fires once per table after pull or sync writes locally; push never emits.
  smugglr.on("table-changed", (detail) => post({ event: "table-changed", detail }));
}

async function addRow(id: string, updatedAt: string) {
  if (!executor) throw new Error("init() first");
  await executor.run(
    "INSERT INTO notes (id, body, updated_at) VALUES (?, ?, ?)",
    [id, `note created at ${updatedAt}`, updatedAt],
  );
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
  // Wipe the IndexedDB database the VFS wrote into.
  await new Promise<void>((resolve, reject) => {
    const req = indexedDB.deleteDatabase(IDB_NAME);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

self.addEventListener("message", async (ev: MessageEvent<Request>) => {
  const { id, op, args } = ev.data;
  try {
    let result: unknown;
    switch (op) {
      case "init": result = await init(args[0] as string, args[1] as string); break;
      case "addRow": result = await addRow(args[0] as string, args[1] as string); break;
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
