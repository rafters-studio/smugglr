// @smugglr/nanostores: a map store as a view on a table of the tab's
// database (FR-SMUGGLR-082), one key per row, keyed by the row's primary
// key. The store loads the table's rows when it mounts, as a task, so
// allTasks() waits for them; each key the store sets is written as its row,
// and each key it removes is a deleted row. When a sync writes rows of the
// table, table-changed names their keys and only those keys are reloaded.
// Syncing is the tab's: the app calls sync().

import { onMount, task } from 'nanostores';

import { table } from './table.js';

/**
 * Binds `store`, a map store, to table `name` of `tab`. `tab` is a Tab
 * from @smugglr/web's open(), or a promise of one. Rendered on a server,
 * it does nothing and the store keeps its initial state.
 */
export function bind(store, { tab, table: name }) {
  if (typeof window === 'undefined') {
    return;
  }
  onMount(store, () => {
    const stops = [];
    let mounted = true;
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
    task(async () => {
      const bound = await table(await tab, name);
      const rows = await bound.all();
      if (!mounted) {
        return;
      }
      put(() => store.set(rows));
      stops.push(
        store.listen((value, old, changed) => {
          if (loading) {
            return;
          }
          const ids = changed === undefined ? new Set([...Object.keys(old ?? {}), ...Object.keys(value)]) : [changed];
          for (const id of ids) {
            if (value[id] === undefined) {
              bound.remove(id);
            } else if (value[id] !== old?.[id]) {
              bound.write(id, value[id]);
            }
          }
        }),
        bound.listen(async (keys) => {
          const found = await bound.some(keys);
          if (mounted) {
            put(() => {
              for (const [id, item] of found) {
                store.setKey(id, item);
              }
            });
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
