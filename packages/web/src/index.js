// @smugglr/web: a tab's own SQLite database, kept in step by the engine in
// the tab with an endpoint the app names (FR-SMUGGLR-062).
//
// open() opens the tab's database in the origin private file system and
// gives it to the app to read and write with SQL. When the tab has no
// memory for its endpoint, open() runs the first sync itself; every later
// sync runs when the app calls sync(), and smugglr starts no timer. A sync,
// a retrieve or a replay that writes rows into the tab fires table-changed
// in every open tab of the origin; a write the app makes itself fires none.
// Each returns its record, the command line's record; a stop is a record,
// never an error.

/**
 * A tab's own database and its sync with the endpoint.
 *
 * Fires `table-changed` (a CustomEvent whose detail is `{ table, keys }`,
 * each key the `quote()` text of its key columns) after smugglr writes rows
 * into the tab, in every open tab of the origin.
 */
export class Tab extends EventTarget {
  #worker;
  #channel;
  #calls = new Map();
  #next = 0;

  /** The record of the first sync open() ran, or null when it ran none. */
  record = null;

  constructor(worker, name) {
    super();
    this.#worker = worker;
    this.#worker.onmessage = ({ data: { id, ok, value, error } }) => {
      const call = this.#calls.get(id);
      this.#calls.delete(id);
      if (ok) {
        call.resolve(value);
      } else {
        call.reject(new Error(error));
      }
    };
    this.#channel = new BroadcastChannel(`smugglr:${name}`);
    this.#channel.onmessage = ({ data }) => {
      if (data?.type === 'table-changed') {
        this.dispatchEvent(new CustomEvent('table-changed', { detail: { table: data.table, keys: data.keys } }));
      }
    };
  }

  /** @internal */
  call(op, ...args) {
    const id = this.#next++;
    return new Promise((resolve, reject) => {
      this.#calls.set(id, { resolve, reject });
      this.#worker.postMessage({ id, op, args });
    });
  }

  /** Reads the tab's database: each row as an array of its values. */
  query(sql, params = []) {
    return this.call('query', sql, params);
  }

  /** Writes the tab's database; returns the count of rows changed. */
  exec(sql, params = []) {
    return this.call('exec', sql, params);
  }

  /** Syncs the tab with its endpoint once; returns the record. */
  sync() {
    return this.call('sync');
  }

  /**
   * Stashes the tab (FR-SMUGGLR-077): returns the blob as JSON text.
   * `multi` is `{ key, node, targets, graveyard }`, the key as 64 hex digits.
   */
  stash(multi) {
    return this.call('stash', multi);
  }

  /** Retrieves blobs (JSON text, one or a list) into the tab; returns the record. */
  retrieve(blobs, multi) {
    return this.call('retrieve', [blobs].flat(), multi);
  }

  /** Re-sends the change a retrieve dropped, by its change hash; returns the record. */
  replay(change, multi) {
    return this.call('replay', change, multi);
  }

  /** Closes the tab's database and its worker. */
  async close() {
    await this.call('close');
    this.#channel.close();
    this.#worker.terminate();
  }
}

/**
 * Opens a tab's own database, kept in step with `endpoint`.
 *
 * @param {object} options
 * @param {string} options.endpoint The URL that answers the place operations.
 * @param {Record<string, string>} [options.headers] The headers the app's auth needs.
 * @param {string[]} [options.ignore] Tables never synced, as names or patterns with * and ?.
 * @param {number} [options.maxWire] The most bytes a first sync moves (100 MB when not set).
 * @param {string} [options.schema] The app's SQL that makes its tables, run before any sync.
 * @param {string} [options.name] The database's file name in the origin private file system.
 */
export async function open({ endpoint, headers = {}, ignore = [], maxWire, schema, name = 'app.db' }) {
  const worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
  const tab = new Tab(worker, name);
  const urls = {
    sqlite: new URL('./sqlite/index.mjs', import.meta.url).href,
    engine: new URL('./engine/smugglr_web.js', import.meta.url).href,
  };
  try {
    tab.record = await tab.call('open', { endpoint, headers, ignore, maxWire, schema, name }, urls);
  } catch (err) {
    worker.terminate();
    throw err;
  }
  return tab;
}
