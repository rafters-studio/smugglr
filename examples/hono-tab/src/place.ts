// The place operations a tab's fetcher asks (FR-SMUGGLR-062), answered over
// the D1 binding for one person's rows: every table with an `owner` column,
// and in it only the rows whose owner is the signed-in person.
//
// Rows travel as SQLite's quote() text of each column, and each row's hash
// is the engine's own (rowHash, from @smugglr/web), so the tab compares
// like with like. A write is applied only where its row is still at the
// change's `from`, all in one D1 batch, the same rule as smugglr's HTTP-SQL
// place.

import { initSync, rowHash } from "../vendor/smugglr-web/src/engine/smugglr_web.js";
import engineWasm from "../vendor/smugglr-web/src/engine/smugglr_web_bg.wasm";

// The engine, compiled at deploy, instantiated once per isolate.
initSync({ module: engineWasm });

/** The column that says whose a row is. */
export const OWNER = "owner";

/** A request the endpoint refuses, with the HTTP status it answers. */
export class Refused extends Error {
  constructor(
    readonly status: 400 | 403,
    message: string,
  ) {
    super(message);
    this.name = "Refused";
  }
}

/** One table, as the fetcher reads it. */
export type Table = {
  name: string;
  columns: string[];
  types: string[];
  key: string[];
  self_keyed: boolean;
};

type Key = string[];

type Change = {
  op: "up" | "down" | "upsert";
  key: Key;
  from: string | null;
  to: string | null;
  values: string[];
};

/** A name as a quoted SQL identifier. */
export const ident = (name: string): string => `"${name.replaceAll('"', '""')}"`;

/** Text as a SQL string literal. */
export const literal = (text: string): string => `'${text.replaceAll("'", "''")}'`;

// SQLite's quote() text: NULL, a number, a string or a blob literal. Only
// text of this form is put into a statement.
const QUOTED = /^(?:NULL|-?\d+(?:\.\d+)?(?:e[+-]?\d+)?|'(?:[^']|'')*'|X'(?:[0-9A-Fa-f]{2})*')$/s;

function quoted(value: unknown): string {
  if (typeof value !== "string" || !QUOTED.test(value)) {
    throw new Refused(400, `${JSON.stringify(value)} is not SQLite quote() text`);
  }
  return value;
}

function texts(value: unknown, what: string): string[] {
  if (!Array.isArray(value)) {
    throw new Refused(400, `${what} is not a list`);
  }
  return value.map(quoted);
}

const quotes = (columns: string[]): string => columns.map((c) => `quote(${ident(c)})`).join(", ");

const hex = (bytes: Uint8Array): string => Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

const hash = (row: string[]): string => hex(rowHash(row));

async function raw(db: D1Database, sql: string, ...params: unknown[]): Promise<unknown[][]> {
  return db.prepare(sql).bind(...params).raw();
}

function strings(rows: unknown[][]): string[][] {
  return rows.map((row) => row.map((value) => String(value)));
}

/** Every table that holds people's rows; D1's and SQLite's own are never named. */
export async function tables(db: D1Database): Promise<Table[]> {
  const names = await raw(
    db,
    "SELECT name FROM sqlite_schema WHERE type = 'table' " +
      "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\' ORDER BY name",
  );
  const found: Table[] = [];
  for (const [name] of names) {
    const table = String(name);
    const info = await raw(db, "SELECT name, pk, type FROM pragma_table_info(?1) ORDER BY cid", table);
    const columns = info.map(([column]) => String(column));
    if (!columns.includes(OWNER)) {
      continue;
    }
    const key = info
      .filter(([, pk]) => Number(pk) > 0)
      .sort(([, a], [, b]) => Number(a) - Number(b));
    let selfKeyed = false;
    const [only] = key;
    if (key.length === 1 && only !== undefined && String(only[2]).toUpperCase() === "INTEGER") {
      const [[indexed] = [0]] = await raw(
        db,
        "SELECT EXISTS (SELECT 1 FROM pragma_index_list(?1) WHERE origin = 'pk')",
        table,
      );
      selfKeyed = Number(indexed) === 0;
    }
    found.push({
      name: table,
      columns,
      types: info.map(([, , type]) => String(type ?? "")),
      key: key.map(([column]) => String(column)),
      self_keyed: selfKeyed,
    });
  }
  return found;
}

/** Each of the person's rows: its key, then its hash as hex. */
async function hashes(db: D1Database, table: Table, owner: string): Promise<[Key, string][]> {
  const rows = await raw(
    db,
    `SELECT ${quotes(table.key)}, ${quotes(table.columns)} FROM ${ident(table.name)} WHERE ${ident(OWNER)} IS ?1`,
    owner,
  );
  return strings(rows).map((row) => [row.slice(0, table.key.length), hash(row.slice(table.key.length))]);
}

/** Each of the person's rows: its key, then the bytes of its quote() text. */
async function weights(db: D1Database, table: Table, owner: string): Promise<[Key, number][]> {
  const bytes = table.columns.map((c) => `length(CAST(quote(${ident(c)}) AS BLOB))`).join(" + ");
  const rows = await raw(
    db,
    `SELECT ${quotes(table.key)}, ${bytes} FROM ${ident(table.name)} WHERE ${ident(OWNER)} IS ?1`,
    owner,
  );
  return rows.map((row) => [row.slice(0, table.key.length).map(String), Number(row[table.key.length])]);
}

const byKey = (table: Table, key: Key): string =>
  // A key column's quote() text is a SQL literal of its value.
  table.key.map((column, i) => `${ident(column)} IS ${key[i]}`).join(" AND ");

// Keys asked in one statement.
const KEYS_A_STATEMENT = 100;

/** The row each key names, as its quote() text, whoever owns it; undefined where there is none. */
async function read(db: D1Database, table: Table, keys: Key[]): Promise<(string[] | undefined)[]> {
  const found = new Map<string, string[]>();
  const keyAt = table.key.map((column) => table.columns.indexOf(column));
  for (let i = 0; i < keys.length; i += KEYS_A_STATEMENT) {
    const values = keys
      .slice(i, i + KEYS_A_STATEMENT)
      .map((key) => `(${key.join(", ")})`)
      .join(", ");
    const rows = await raw(
      db,
      `SELECT ${quotes(table.columns)} FROM ${ident(table.name)} ` +
        `WHERE (${table.key.map(ident).join(", ")}) IN (VALUES ${values})`,
    );
    for (const row of strings(rows)) {
      found.set(JSON.stringify(keyAt.map((at) => row[at])), row);
    }
  }
  return keys.map((key) => found.get(JSON.stringify(key)));
}

function change(value: unknown, table: Table): Change {
  if (typeof value !== "object" || value === null) {
    throw new Refused(400, "a change is not an object");
  }
  const { op, key, from, to, values } = value as Record<string, unknown>;
  if (op !== "up" && op !== "down" && op !== "upsert") {
    throw new Refused(400, `${JSON.stringify(op)} is not a change`);
  }
  const hashOf = (h: unknown) => (typeof h === "string" && /^[0-9a-fA-F]{32}$/.test(h) ? h.toLowerCase() : null);
  const parsed: Change = {
    op,
    key: texts(key, "a change's key"),
    from: hashOf(from),
    to: hashOf(to),
    values: op === "down" ? [] : texts(values, "a change's values"),
  };
  if (parsed.key.length !== table.key.length || (op !== "down" && parsed.values.length !== table.columns.length)) {
    throw new Refused(400, `a change that does not fit ${table.name}`);
  }
  return parsed;
}

/**
 * Writes the changes, each only where its row is still at its `from`, and
 * returns the keys written. A change to another person's row, or one that
 * gives a row another owner, refuses the whole write.
 */
async function write(db: D1Database, table: Table, owner: string, sent: unknown): Promise<Key[]> {
  if (!Array.isArray(sent)) {
    throw new Refused(400, "changes is not a list");
  }
  const changes = sent.map((c) => change(c, table));
  const ownerAt = table.columns.indexOf(OWNER);
  const mine = literal(owner);
  const here = await read(
    db,
    table,
    changes.map((c) => c.key),
  );
  const statements: D1PreparedStatement[] = [];
  const keys: Key[] = [];
  changes.forEach((c, i) => {
    const row = here[i];
    if ((row !== undefined && row[ownerAt] !== mine) || (c.op !== "down" && c.values[ownerAt] !== mine)) {
      throw new Refused(403, `${table.name} ${c.key.join(", ")} is not the signed-in person's`);
    }
    const sql = statement(table, c, row);
    if (sql !== undefined) {
      statements.push(db.prepare(`${sql} RETURNING 1`));
      keys.push(c.key);
    }
  });
  if (statements.length === 0) {
    return [];
  }
  const results = await db.batch(statements);
  return keys.filter((_, i) => (results[i]?.results.length ?? 0) > 0);
}

/** The statement that writes `c` where its row is still `row`; none when it is not at `from`. */
function statement(table: Table, c: Change, row: string[] | undefined): string | undefined {
  const name = ident(table.name);
  const asRead = (current: string[]) =>
    table.columns.map((column, i) => `quote(${ident(column)}) IS ${literal(current[i] ?? "NULL")}`).join(" AND ");
  if (c.op === "up" || (c.op === "upsert" && c.from === null)) {
    return (
      `INSERT INTO ${name} (${table.columns.map(ident).join(", ")}) VALUES (${c.values.join(", ")}) ` +
      `ON CONFLICT (${table.key.map(ident).join(", ")}) DO NOTHING`
    );
  }
  if (row === undefined || hash(row) !== c.from) {
    return undefined;
  }
  if (c.op === "upsert") {
    const set = table.columns
      .map((column, i) => [column, c.values[i]] as const)
      .filter(([column]) => !table.key.includes(column))
      .map(([column, value]) => `${ident(column)} = ${value}`);
    const assignments = set.length > 0 ? set : table.key.map((k) => `${ident(k)} = ${ident(k)}`);
    return `UPDATE ${name} SET ${assignments.join(", ")} WHERE ${byKey(table, c.key)} AND ${asRead(row)}`;
  }
  return `DELETE FROM ${name} WHERE ${byKey(table, c.key)} AND ${asRead(row)}`;
}

/** Answers one place operation for the person `owner`. */
export async function answer(db: D1Database, owner: string, request: unknown): Promise<unknown> {
  if (typeof request !== "object" || request === null) {
    throw new Refused(400, "a request is one place operation, as JSON");
  }
  const { op, table: named, keys, changes } = request as Record<string, unknown>;
  const all = await tables(db);
  if (op === "tables") {
    return all;
  }
  const table = all.find((t) => t.name === named);
  if (table === undefined) {
    throw new Refused(400, `${JSON.stringify(named)} is not a table here`);
  }
  switch (op) {
    case "hashes":
      return hashes(db, table, owner);
    case "weights":
      return weights(db, table, owner);
    case "rows": {
      if (!Array.isArray(keys)) {
        throw new Refused(400, "keys is not a list");
      }
      const asked = keys.map((key) => texts(key, "a key"));
      const mine = literal(owner);
      const ownerAt = table.columns.indexOf(OWNER);
      return (await read(db, table, asked)).filter((row) => row !== undefined && row[ownerAt] === mine);
    }
    case "write":
      return write(db, table, owner, changes);
    default:
      throw new Refused(400, `${JSON.stringify(op)} is not a place operation`);
  }
}
