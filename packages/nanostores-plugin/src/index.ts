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

import type { Smugglr, SqlExecutor } from "smugglr";
import { createPersistBinding } from "smugglr";
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

  const binding = createPersistBinding<T>({
    smugglr: options.smugglr,
    executor: options.executor,
    table: options.table,
    key: options.key,
    serialize,
    deserialize,
    applyHydrated: (parsed) => writable.set(parsed),
    onHydrate: options.onHydrate,
    subscribe: (notify) => {
      // nanostores' subscribe() fires synchronously with the current value
      // at attach time. That's not a "change" -- we don't want to persist
      // the pre-hydration default and stomp the row before hydrate() can
      // read it.
      let seenInitialFire = false;
      return writable.subscribe((value) => {
        if (!seenInitialFire) {
          seenInitialFire = true;
          return;
        }
        notify(value);
      });
    },
    logPrefix: "[@smugglr/nanostores]",
  });

  return () => {
    binding.dispose();
  };
}
