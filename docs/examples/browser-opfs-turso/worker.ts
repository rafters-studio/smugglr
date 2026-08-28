// Worker: owns wa-sqlite + OPFS + smugglr. The page proxies via postMessage.
// OPFS sync access handles are worker-only in WebKit and Firefox, so the
// database and the sync client live here rather than on the main thread.

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
  const vfs = new OriginPrivateFileSystemVFS();
  await new Promise((r) => setTimeout(r, 0));
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(
    "demo.db",
    SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
    "opfs",
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
  const root = await navigator.storage.getDirectory();
  for await (const entry of root.values()) {
    await root.removeEntry(entry.name, { recursive: true });
  }
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
