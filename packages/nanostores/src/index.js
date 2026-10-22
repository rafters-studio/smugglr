// @smugglr/nanostores: a map store as a view on a table of the tab's
// database (FR-SMUGGLR-082), one key per row, keyed by the row's primary
// key. The store loads the table's rows when it mounts, as a task, so
// allTasks() waits for them; each key the store sets is written as its row,
// and each key it removes is a deleted row. A key set before the load
// finishes is kept and written once it has. A write the tab refuses puts the
// key back to what the table holds and is reported to onError. When a sync
// writes rows of the table, table-changed names their keys and only those
// keys are reloaded. Syncing is the tab's: the app calls sync().

import { onMount, task } from 'nanostores';

import { table } from './table.js';

const raise = (err) => {
  throw err;
};

/**
 * Binds `store`, a map store, to table `name` of `tab`. `tab` is a Tab
 * from @smugglr/web's open(), or a promise of one; `onError` hears each
 * write or remove the tab refused. Rendered on a server, it does nothing and
 * the store keeps its initial state.
 */
export function bind(store, { tab, table: name, onError = raise }) {
  if (typeof window === 'undefined') {
    return;
  }
  onMount(store, () => {
    const stops = [];
    let mounted = true;
    let bound;
    // Keys the store changed before the load finished.
    const pending = new Set();
    // When the store last changed each key, as a count of its changes; a
    // reload skips a key the store changed after the reload began.
    let clock = 0;
    const touched = new Map();
    // Rows put into the store from the database are not written back.
    let loading = false;
    const put = (change) => {
      loading = true;
      try {
        change();
      } finally {
        loading = false;
      }
    };
    const putOne = (id, item) => put(() => store.setKey(id, item));
    stops.push(
      store.listen((value, old, changed) => {
        if (loading) {
          return;
        }
        const ids = changed === undefined ? new Set([...Object.keys(old ?? {}), ...Object.keys(value)]) : [changed];
        for (const id of ids) {
          if (value[id] === old?.[id]) {
            continue;
          }
          touched.set(id, ++clock);
          if (bound === undefined) {
            pending.add(id);
          } else {
            bound.save(id, value[id], putOne, onError);
          }
        }
      }),
    );
    task(async () => {
      const loaded = await table(await tab, name);
      const rows = await loaded.all();
      if (!mounted) {
        return;
      }
      const value = store.get();
      for (const id of pending) {
        if (value[id] === undefined) {
          delete rows[id];
        } else {
          rows[id] = value[id];
        }
      }
      put(() => store.set(rows));
      bound = loaded;
      for (const id of pending) {
        bound.save(id, value[id], putOne, onError);
      }
      pending.clear();
      stops.push(
        bound.listen(async (keys) => {
          const began = clock;
          const found = await bound.some(keys);
          if (mounted) {
            for (const [id, item] of found) {
              if (!(touched.get(id) > began)) {
                putOne(id, item);
              }
            }
          }
        }),
      );
    });
    return () => {
      mounted = false;
      for (const stop of stops) {
        stop();
      }
    };
  });
}
