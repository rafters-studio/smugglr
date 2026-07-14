// Shared persistence core for smugglr's state-library plugins (zustand,
// nanostores, and any future adapter). Owns the SQL constants, the
// persist/hydrate round trip, the dedup-by-lastSerialized + suppress-loop
// guard, and the `table-changed` event matcher. Each plugin keeps its own
// store-specific glue (zustand's `include()` projection, nanostores'
// `seenInitialFire` skip) and wires it in through `subscribe` /
// `applyHydrated` rather than having it flattened into this module.
//
// Persistence shape (one row per key, multiple keys can share a table):
//   CREATE TABLE <table> (
//     key TEXT PRIMARY KEY,
//     value TEXT NOT NULL,
//     updated_at TEXT
//   );

import type { SqlExecutor, TableChangedEvent, Unsubscribe } from "./types.js";
import { quoteIdent } from "./autoSync.js";

/** Minimal event source contract -- satisfied by `Pick<Smugglr, "on">`. */
export interface PersistBindingSource {
  on(
    event: "table-changed",
    handler: (event: TableChangedEvent) => void,
  ): Unsubscribe;
}

export interface CreatePersistBindingOptions<T> {
  /** Smugglr instance (or a stub) used for change-event subscription. */
  smugglr: PersistBindingSource;
  /** Local SQL executor used for direct read/write of the persistence row. */
  executor: SqlExecutor;
  /**
   * Table that stores the persisted row. Caller owns the DDL:
   * `CREATE TABLE <table> (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT)`.
   * Validated with the same identifier guard autoSync uses (`quoteIdent`);
   * an unsafe table name throws at construction time.
   */
  table: string;
  /** Primary key for this binding's row. Use distinct keys when multiple bindings share a table. */
  key: string;
  /** Serializer for the persisted value. */
  serialize: (value: T) => string;
  /** Parser for the persisted value. */
  deserialize: (raw: string) => T;
  /**
   * Applies a hydrated value to the underlying store/atom. Called with the
   * suppress-next-write guard already armed, so if this triggers a
   * synchronous write back through `subscribe`, that write is swallowed
   * instead of looping into a redundant persist.
   */
  applyHydrated: (value: T) => void;
  /** Called once after every hydrate attempt, whether or not a row existed. */
  onHydrate?: (hydrated: T | null) => void;
  /**
   * Attaches a listener for local writes that are candidates for
   * persistence. Callers apply their own store-specific pre-filtering here
   * (zustand's `include()` projection, nanostores' `seenInitialFire` skip
   * on the synchronous first fire) before invoking `notify`. Must return an
   * unsubscribe function.
   */
  subscribe: (notify: (value: T) => void) => Unsubscribe;
  /** Prefix used on `console.warn` messages, e.g. "[@smugglr/zustand]". */
  logPrefix: string;
}

export interface PersistBinding {
  /**
   * Detaches the store listener and the `table-changed` listener. Does not
   * roll back the persisted row -- the data stays put.
   */
  dispose(): void;
}

/**
 * Shared persist/hydrate core for smugglr's state-library plugins.
 *
 * Runs an initial hydrate on construction (fire-and-forget, matching the
 * plugins' prior behavior).
 *
 * @example
 * ```ts
 * const binding = createPersistBinding<State>({
 *   smugglr,
 *   executor,
 *   table: "app_state",
 *   key: "todos",
 *   serialize: JSON.stringify,
 *   deserialize: JSON.parse,
 *   applyHydrated: (v) => store.set(v),
 *   subscribe: (notify) => store.subscribe(notify),
 *   logPrefix: "[@smugglr/example]",
 * });
 * ```
 */
export function createPersistBinding<T>(
  options: CreatePersistBindingOptions<T>,
): PersistBinding {
  const safeTable = quoteIdent(options.table);
  const upsertSql = `INSERT INTO ${safeTable} (key, value, updated_at) VALUES (?, ?, ?)
   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`;
  const hydrateSql = `SELECT value FROM ${safeTable} WHERE key = ? LIMIT 1`;

  let suppressNextWrite = false;
  let lastSerialized: string | null = null;

  const persist = async (value: T): Promise<void> => {
    const next = options.serialize(value);
    if (next === lastSerialized) return;
    lastSerialized = next;
    try {
      await options.executor.run(upsertSql, [options.key, next, new Date().toISOString()]);
    } catch (err) {
      console.warn(`${options.logPrefix} persist failed:`, err);
    }
  };

  const hydrate = async (): Promise<void> => {
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
      const parsed = options.deserialize(raw);
      lastSerialized = raw;
      suppressNextWrite = true;
      options.applyHydrated(parsed);
      options.onHydrate?.(parsed);
    } catch (err) {
      console.warn(`${options.logPrefix} hydrate failed:`, err);
      options.onHydrate?.(null);
    }
  };

  const unsubStore = options.subscribe((value) => {
    if (suppressNextWrite) {
      suppressNextWrite = false;
      return;
    }
    void persist(value);
  });

  const unsubSmugglr = options.smugglr.on("table-changed", (event: TableChangedEvent) => {
    if (event.table !== options.table) return;
    if (!event.changedPks.includes(options.key)) return;
    void hydrate();
  });

  void hydrate();

  return {
    dispose() {
      unsubStore();
      unsubSmugglr();
    },
  };
}
