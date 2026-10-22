// @smugglr/zustand: a middleware that makes one collection of a zustand
// store's state a view on a table of the tab's database (FR-SMUGGLR-082):
// the collection is an object keyed by each row's primary key, one item per
// row. The store loads the table's rows when it is created; each item the
// app changes is found by comparing the previous and next item per key, and
// is written as its row, or deleted. When a sync writes rows of the table,
// table-changed names their keys and only those keys are reloaded. Syncing
// is the tab's: the app calls sync().

import { table } from './table.js';

/**
 * The middleware: `createStore(smugglr(config, { tab, table, collection }))`.
 * `tab` is a Tab from @smugglr/web's open(), or a promise of one;
 * `collection` names the state's field that holds the table's rows. The
 * store offers `store.smugglr.hasHydrated()` and
 * `store.smugglr.onFinishHydration(listener)`, as zustand's persist names
 * them. Rendered on a server, it reads and writes nothing and the store keeps
 * its initial state.
 */
export const smugglr =
  (config, { tab, table: name, collection }) =>
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
    (async () => {
      const bound = await table(await tab, name);
      put(await bound.all());
      api.subscribe((next, previous) => {
        const [after, before] = [next[collection] ?? {}, previous[collection] ?? {}];
        if (loading || after === before) {
          return;
        }
        for (const id of new Set([...Object.keys(before), ...Object.keys(after)])) {
          if (after[id] === undefined) {
            bound.remove(id);
          } else if (after[id] !== before[id]) {
            bound.write(id, after[id]);
          }
        }
      });
      bound.listen(async (keys) => {
        const items = { ...get()[collection] };
        for (const [id, item] of await bound.some(keys)) {
          if (item === undefined) {
            delete items[id];
          } else {
            items[id] = item;
          }
        }
        put(items);
      });
      hydrated = true;
      for (const listener of finished) {
        listener(get());
      }
    })();
    return state;
  };
