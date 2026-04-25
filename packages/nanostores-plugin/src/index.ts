// @smugglr/nanostores -- nanostores adapter for smugglr.
//
// Persists atom / map values to a smugglr-managed SQLite table and
// rehydrates whenever a sync event reports the row as changed.
//
// Persistence shape (one row per atom, multiple atoms can share a table):
//   CREATE TABLE <table> (
//     key TEXT PRIMARY KEY,
//     value TEXT NOT NULL,
//     updated_at TEXT
//   );

import type { Smugglr, SqlExecutor, TableChangedEvent } from "smugglr";
import type { Atom, MapStore, ReadableAtom } from "nanostores";

export interface SmugglOptions<T> {
  /** Smugglr instance used for change-event subscription. */
  smugglr: Pick<Smugglr, "on">;
  /** Local SQL executor used for direct read/write of the persistence row. */
  executor: SqlExecutor;
  /**
   * Persistence table. Caller owns the DDL:
   * `CREATE TABLE <table> (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT)`.
   */
  table: string;
  /** Primary key for this atom's row. Use distinct keys when multiple atoms share one table. */
  key: string;
  /** Custom JSON serializer. Defaults to `JSON.stringify`. */
  serialize?: (value: T) => string;
  /** Custom JSON parser. Defaults to `JSON.parse`. */
  deserialize?: (raw: string) => T;
  /** Called once after the initial hydration query completes. */
  onHydrate?: (hydrated: T | null) => void;
}

const HYDRATE_SQL = (table: string) =>
  `SELECT value FROM "${table}" WHERE key = ? LIMIT 1`;

const UPSERT_SQL = (table: string) =>
  `INSERT INTO "${table}" (key, value, updated_at) VALUES (?, ?, ?)
   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`;

/** A nanostore that supports `.get()` and `.set()` -- atom or map. */
type WritableStore<T> = ReadableAtom<T> & { set(value: T): void };

/**
 * Persist a nanostore to a smugglr-managed SQLite row.
 *
 * Returns an unsubscribe function that detaches the listener. Calling it
 * does *not* roll back the persisted row -- the data stays put.
 *
 * @example
 * ```ts
 * import { atom } from "nanostores";
 * import { smuggl } from "@smugglr/nanostores";
 *
 * const $todos = atom<Todo[]>([]);
 * const dispose = smuggl($todos, {
 *   smugglr,
 *   executor,
 *   table: "app_state",
 *   key: "todos",
 * });
 * ```
 */
export function smuggl<T>(
  store: Atom<T> | MapStore<T extends object ? T : never> | WritableStore<T>,
  options: SmugglOptions<T>,
): () => void {
  const writable = store as WritableStore<T>;
  const serialize = options.serialize ?? JSON.stringify;
  const deserialize = options.deserialize ?? JSON.parse;
  const upsertSql = UPSERT_SQL(options.table);
  const hydrateSql = HYDRATE_SQL(options.table);

  let suppressNextWrite = false;
  // nanostores' subscribe() fires synchronously with the current value at
  // attach time. That's not a "change" -- we don't want to persist the
  // pre-hydration default and stomp the row before hydrate() can read it.
  let seenInitialFire = false;
  let lastSerialized: string | null = null;

  const persist = async (value: T) => {
    const next = serialize(value);
    if (next === lastSerialized) return;
    lastSerialized = next;
    try {
      await options.executor.run(upsertSql, [options.key, next, new Date().toISOString()]);
    } catch (err) {
      console.warn("[@smugglr/nanostores] persist failed:", err);
    }
  };

  const hydrate = async () => {
    try {
      const result = await options.executor.run(hydrateSql, [options.key]);
      if (result.rows.length === 0) {
        options.onHydrate?.(null);
        return;
      }
      const raw = result.rows[0][0];
      if (typeof raw !== "string") {
        options.onHydrate?.(null);
        return;
      }
      const parsed = deserialize(raw) as T;
      lastSerialized = raw;
      suppressNextWrite = true;
      writable.set(parsed);
      options.onHydrate?.(parsed);
    } catch (err) {
      console.warn("[@smugglr/nanostores] hydrate failed:", err);
      options.onHydrate?.(null);
    }
  };

  const unsubStore = writable.subscribe((value) => {
    if (!seenInitialFire) {
      seenInitialFire = true;
      return;
    }
    if (suppressNextWrite) {
      suppressNextWrite = false;
      return;
    }
    void persist(value);
  });

  const unsubSmugglr = options.smugglr.on(
    "table-changed",
    (event: TableChangedEvent) => {
      if (event.table !== options.table) return;
      if (!event.changedPks.includes(options.key)) return;
      void hydrate();
    },
  );

  void hydrate();

  return () => {
    unsubStore();
    unsubSmugglr();
  };
}
