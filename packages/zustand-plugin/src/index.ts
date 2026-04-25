// @smugglr/zustand -- Zustand middleware that auto-persists store slices to a
// smugglr-managed SQLite table and rehydrates from sync events.
//
// Storage shape (one row per slice, multiple slices share one table):
//   CREATE TABLE <table> (
//     key TEXT PRIMARY KEY,
//     value TEXT NOT NULL,
//     updated_at TEXT
//   );

import type { Smugglr, SqlExecutor, TableChangedEvent } from "smugglr";
import type { StateCreator, StoreMutatorIdentifier } from "zustand";

export interface SmugglOptions<T, U = T> {
  /** Smugglr instance used for change-event subscription. */
  smugglr: Pick<Smugglr, "on">;
  /** Local SQL executor used for direct read/write of the persistence row. */
  executor: SqlExecutor;
  /**
   * Table that stores the persisted slice. Caller owns the DDL:
   * `CREATE TABLE <table> (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT)`.
   */
  table: string;
  /** Primary key for this store's row. Use distinct keys when multiple stores share the same table. */
  key: string;
  /**
   * Optional projector. The middleware persists `include(state)` instead of the
   * full state. Defaults to identity. Use this to skip ephemeral fields
   * (e.g. transient UI state) from the persisted slice.
   */
  include?: (state: T) => U;
  /** Custom JSON serializer. Defaults to `JSON.stringify`. */
  serialize?: (value: U) => string;
  /** Custom JSON parser. Defaults to `JSON.parse`. */
  deserialize?: (raw: string) => U;
  /** Called once after the initial hydration query completes. */
  onHydrate?: (hydrated: U | null) => void;
}

type Mutators = [StoreMutatorIdentifier, unknown][];

export type Smuggl = <
  T,
  Mps extends Mutators = [],
  Mcs extends Mutators = [],
  U = T,
>(
  initializer: StateCreator<T, Mps, Mcs>,
  options: SmugglOptions<T, U>,
) => StateCreator<T, Mps, Mcs>;

const HYDRATE_SQL = (table: string) =>
  `SELECT value FROM "${table}" WHERE key = ? LIMIT 1`;

const UPSERT_SQL = (table: string) =>
  `INSERT INTO "${table}" (key, value, updated_at) VALUES (?, ?, ?)
   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`;

/**
 * Zustand middleware that persists a store slice to a smugglr-managed table
 * and rehydrates whenever a sync event touches the same row.
 *
 * @example
 * ```ts
 * const useStore = create<AppState>()(
 *   smuggl(
 *     (set) => ({
 *       todos: [],
 *       addTodo: (t) => set((s) => ({ todos: [...s.todos, t] })),
 *     }),
 *     {
 *       smugglr,
 *       executor,
 *       table: "app_state",
 *       key: "todos",
 *       include: (s) => ({ todos: s.todos }),
 *     },
 *   ),
 * );
 * ```
 */
export const smuggl: Smuggl = (initializer, options) => (set, get, api) => {
  const include = options.include ?? ((s: unknown) => s as never);
  const serialize = options.serialize ?? JSON.stringify;
  const deserialize = options.deserialize ?? JSON.parse;
  const upsertSql = UPSERT_SQL(options.table);
  const hydrateSql = HYDRATE_SQL(options.table);

  let suppressNextWrite = false;
  let lastSerialized: string | null = null;

  // Build the underlying store first; subscribe + hydrate after.
  const state = initializer(set, get, api);

  const persist = async (raw: unknown) => {
    const slice = include(raw as never);
    const next = serialize(slice);
    if (next === lastSerialized) return;
    lastSerialized = next;
    try {
      await options.executor.run(upsertSql, [options.key, next, new Date().toISOString()]);
    } catch (err) {
      console.warn("[@smugglr/zustand] persist failed:", err);
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
      const parsed = deserialize(raw);
      lastSerialized = raw;
      suppressNextWrite = true;
      // Merge the hydrated slice into the live store.
      set(parsed as never, false);
      options.onHydrate?.(parsed);
    } catch (err) {
      console.warn("[@smugglr/zustand] hydrate failed:", err);
      options.onHydrate?.(null);
    }
  };

  api.subscribe((next) => {
    if (suppressNextWrite) {
      suppressNextWrite = false;
      return;
    }
    void persist(next);
  });

  options.smugglr.on("table-changed", (event: TableChangedEvent) => {
    if (event.table !== options.table) return;
    if (!event.changedPks.includes(options.key)) return;
    void hydrate();
  });

  void hydrate();

  return state;
};
