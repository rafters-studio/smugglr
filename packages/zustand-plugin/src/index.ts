// @smugglr/zustand -- Zustand middleware that auto-persists store slices to a
// smugglr-managed SQLite table and rehydrates from sync events.
//
// Storage shape (one row per slice, multiple slices share one table):
//   CREATE TABLE <table> (
//     key TEXT PRIMARY KEY,
//     value TEXT NOT NULL,
//     updated_at TEXT
//   );

import type { Smugglr, SqlExecutor } from "smugglr";
import { createPersistBinding } from "smugglr";
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

  // Build the underlying store first; subscribe + hydrate after.
  const state = initializer(set, get, api);

  createPersistBinding({
    smugglr: options.smugglr,
    executor: options.executor,
    table: options.table,
    key: options.key,
    serialize,
    deserialize,
    // Merge the hydrated slice into the live store.
    applyHydrated: (parsed) => set(parsed as never, false),
    onHydrate: options.onHydrate,
    // include() is zustand's projection: persist include(state), not the
    // full state.
    subscribe: (notify) => api.subscribe((next) => notify(include(next as never))),
    logPrefix: "[@smugglr/zustand]",
  });

  return state;
};
