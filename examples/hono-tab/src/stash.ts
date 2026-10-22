// A stash blob of one person's rows (FR-SMUGGLR-077), for a tab to fill
// itself with retrieve(). The Worker reads the rows from D1 and hands them
// to the engine (stashRows, from @smugglr/web), whose own stash makes the
// blob, signed with the app's key: no blob format is written here.

import { stashRows } from "../vendor/smugglr-web/src/engine/smugglr_web.js";
import { ident, OWNER, type Table } from "./place";

/** What a stash names: the app's key (64 hex digits), this node, and the device it is for. */
export type Multi = { key: string; node: string; target: string };

/** Stashes the person `owner`'s rows of `tables`; returns the blob as JSON text. */
export async function stash(db: D1Database, tables: Table[], owner: string, multi: Multi): Promise<string> {
  const handed = [];
  for (const table of tables) {
    // Each value as its quote() text, so nothing is lost on the way,
    // integers beyond 2^53 included.
    const rows = await db
      .prepare(
        `SELECT ${table.columns.map((c) => `quote(${ident(c)})`).join(", ")} ` +
          `FROM ${ident(table.name)} WHERE ${ident(OWNER)} IS ?1`,
      )
      .bind(owner)
      .raw<string[]>();
    handed.push({ ...table, rows });
  }
  const settings = { key: multi.key, node: multi.node, targets: [multi.target], now: Math.floor(Date.now() / 1000) };
  return stashRows(JSON.stringify(handed), JSON.stringify(settings));
}
