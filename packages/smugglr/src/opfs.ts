// wa-sqlite executor adapter.
//
// Wraps a wa-sqlite database handle in the SqlExecutor shape that smugglr
// expects. The user owns wa-sqlite setup (factory, VFS registration, db
// open); this adapter only translates run(sql, params) calls into wa-sqlite
// statement iteration and result collection.

import type { SqlExecutor } from "./types.js";

/**
 * Minimal subset of the wa-sqlite API surface this adapter calls into.
 * Typed loosely so consumers can pass a wa-sqlite handle without us
 * depending on @types/wa-sqlite (which doesn't ship official typings).
 */
export interface WaSqlite3 {
  statements(db: number, sql: string): AsyncIterable<number>;
  bind(stmt: number, idx: number, value: unknown): void;
  column_count(stmt: number): number;
  column_name(stmt: number, idx: number): string;
  column(stmt: number, idx: number): unknown;
  step(stmt: number): Promise<number>;
  /** SQLite result code for a row being available. */
  SQLITE_ROW?: number;
}

/**
 * Wrap a wa-sqlite database handle as a smugglr SqlExecutor.
 *
 * Usage (browser, OPFS-backed):
 * ```ts
 * import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
 * import * as SQLite from "wa-sqlite";
 * import { OPFSCoopSyncVFS } from "wa-sqlite/src/examples/OPFSCoopSyncVFS.js";
 * import { Smugglr, createWaSqliteExecutor } from "smugglr";
 *
 * const module = await SQLiteAsyncESMFactory();
 * const sqlite3 = SQLite.Factory(module);
 * const vfs = await OPFSCoopSyncVFS.create("opfs", module);
 * sqlite3.vfs_register(vfs, true);
 * const db = await sqlite3.open_v2(
 *   "app.db",
 *   SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
 *   "opfs",
 * );
 *
 * const s = await Smugglr.init({
 *   source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
 *   dest: { url: "https://my-db.turso.io", authToken: "...", profile: "turso" },
 *   sync: { tables: ["users", "posts"] },
 * });
 *
 * await s.sync();
 * ```
 *
 * @param sqlite3 wa-sqlite namespace from `SQLite.Factory(module)`
 * @param db database handle from `sqlite3.open_v2(...)`
 */
export function createWaSqliteExecutor(
  sqlite3: WaSqlite3,
  db: number,
): SqlExecutor {
  const SQLITE_ROW = sqlite3.SQLITE_ROW ?? 100;

  return {
    async run(sql, params) {
      const columns: string[] = [];
      const rows: unknown[][] = [];
      let columnsCaptured = false;

      for await (const stmt of sqlite3.statements(db, sql)) {
        for (let i = 0; i < params.length; i++) {
          sqlite3.bind(stmt, i + 1, params[i]);
        }

        const colCount = sqlite3.column_count(stmt);
        if (!columnsCaptured && colCount > 0) {
          for (let i = 0; i < colCount; i++) {
            columns.push(sqlite3.column_name(stmt, i));
          }
          columnsCaptured = true;
        }

        while ((await sqlite3.step(stmt)) === SQLITE_ROW) {
          const row: unknown[] = [];
          for (let i = 0; i < colCount; i++) {
            row.push(sqlite3.column(stmt, i));
          }
          rows.push(row);
        }
      }

      return { columns, rows };
    },
  };
}
