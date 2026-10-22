// @smugglr/zustand: a middleware that makes one collection of a zustand
// store's state a view on a table of the tab's database (FR-SMUGGLR-082):
// the collection is an object keyed by each row's primary key, one item per
// row. The store loads the table's rows when it is created; each item the
// app changes is found by comparing the previous and next item per key, and
// is written as its row, or deleted. An item changed before the load
// finishes is kept and written once it has. A write the tab refuses puts the
// item back to what the table holds and is reported to onError. When a sync
// writes rows of the table, table-changed names their keys and only those
// keys are reloaded. Syncing is the tab's: the app calls sync().

import { table } from './table.js';

const raise = (err) => {
  throw err;
};

/**
 * The middleware:
 * `createStore(smugglr(config, { tab, table, collection, onError }))`.
 * `tab` is a Tab from @smugglr/web's open(), or a promise of one;
 * `collection` names the state's field that holds the table's rows;
 * `onError` hears each write or remove the tab refused. The store offers
 * `store.smugglr.hasHydrated()` and `store.smugglr.onFinishHydration(listener)`,
 * as zustand's persist names them. Rendered on a server, it reads and writes
 * nothing and the store keeps its initial state.
 */
export const smugglr =
  (config, { tab, table: name, collection, onError = raise }) =>
  (set, get, api) => {
    let hydrated = false;
    const finished = new Set();
    api.smugglr = {
      hasHydrated: () => hydrated,
      onFinishHydration(listener) {
        finished.add(listener);
        return () => finished.delete(listener);
      },
    };
    const state = config(set, get, api);
    if (typeof window === 'undefined') {
      return state;
    }
    let bound;
    // Items the app changed before the load finished.
    const pending = new Set();
    // When the app last changed each item, as a count of its changes; a
    // reload skips an item the app changed after the reload began.
    let clock = 0;
    const touched = new Map();
    // Rows put into the store from the database are not written back.
    let loading = false;
    const put = (items) => {
      loading = true;
      try {
        api.setState({ [collection]: items });
      } finally {
        loading = false;
      }
    };
    // Sets the item of each [id, item], deleting it when undefined, and no other.
    const putSome = (found) => {
      const items = { ...get()[collection] };
      for (const [id, item] of found) {
        if (item === undefined) {
          delete items[id];
        } else {
          items[id] = item;
        }
      }
      put(items);
    };
    const putOne = (id, item) => putSome([[id, item]]);
    api.subscribe((next, previous) => {
      const [after, before] = [next[collection] ?? {}, previous[collection] ?? {}];
      if (loading || after === before) {
        return;
      }
      for (const id of new Set([...Object.keys(before), ...Object.keys(after)])) {
        if (after[id] === before[id]) {
          continue;
        }
        touched.set(id, ++clock);
        if (bound === undefined) {
          pending.add(id);
        } else {
          bound.save(id, after[id], putOne, onError);
        }
      }
    });
    (async () => {
      const loaded = await table(await tab, name);
      const rows = await loaded.all();
      const current = get()[collection] ?? {};
      for (const id of pending) {
        if (current[id] === undefined) {
          delete rows[id];
        } else {
          rows[id] = current[id];
        }
      }
      put(rows);
      bound = loaded;
      for (const id of pending) {
        bound.save(id, current[id], putOne, onError);
      }
      pending.clear();
      bound.listen(async (keys) => {
        const began = clock;
        const found = await bound.some(keys);
        putSome(found.filter(([id]) => !(touched.get(id) > began)));
      });
      hydrated = true;
      for (const listener of finished) {
        listener(get());
      }
    })();
    return state;
  };
