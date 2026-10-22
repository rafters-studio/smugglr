// The tab's worker (FR-SMUGGLR-062): it holds the tab's own SQLite database
// and smugglr's memory, two databases side by side in the origin private
// file system, and runs the engine (smugglr-web, compiled to WASM) on them.
//
// The engine is synchronous, as a place is, so the worker gives it
// synchronous calls on globalThis.smugglrHost: HTTP-SQL on the tab's
// connection, memory's files, and the fetcher's HTTP (a synchronous
// XMLHttpRequest, which a worker may make). One run at a time holds the Web
// Lock for the tab's database, across every tab of the origin; a run that
// writes rows into the tab posts table-changed on a BroadcastChannel, which
// every tab of the origin hears.

let tab;
let engine;
let app;
let memory;
let channel;

const words = (err) => String(err?.message ?? err);

// An HTTP-SQL parameter as SQLite binds it.
function bound(param) {
  if (param !== null && typeof param === 'object') {
    if (param.$type === 'bigint') {
      return BigInt(param.$value);
    }
    if (param.$type === 'blob') {
      return Uint8Array.from(atob(param.$value), (c) => c.charCodeAt(0));
    }
  }
  return param;
}

// A value as JSON carries it: an integer beyond 2^53 as its text.
function plain(value) {
  if (typeof value === 'bigint') {
    return Number.isSafeInteger(Number(value)) ? Number(value) : value.toString();
  }
  return value;
}

function run(db, { sql, params }) {
  const bind = params?.length ? params.map(bound) : undefined;
  const rows = db.exec({ sql, bind, rowMode: 'array', returnValue: 'resultRows' });
  return { rows: rows.map((row) => row.map(plain)), rowsAffected: db.changes() };
}

function transaction(db, body) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const result = body();
    db.exec('COMMIT');
    return result;
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
}

function host() {
  return {
    sql(text) {
      try {
        const request = JSON.parse(text);
        if (request.batch === undefined) {
          return JSON.stringify(run(app, request));
        }
        const all = () => request.batch.map((statement) => run(app, statement));
        const results = request.atomic ? transaction(app, all) : all();
        return JSON.stringify({ results });
      } catch (err) {
        throw words(err);
      }
    },
    fetch(body) {
      const request = new XMLHttpRequest();
      try {
        request.open('POST', tab.endpoint, false);
        request.setRequestHeader('Content-Type', 'application/json');
        for (const [name, value] of Object.entries(tab.headers)) {
          request.setRequestHeader(name, value);
        }
        request.send(body);
      } catch (err) {
        throw words(err);
      }
      if (request.status !== 200) {
        throw `${request.status} ${request.responseText}`;
      }
      return request.responseText;
    },
    memRead(path) {
      try {
        const rows = memory.selectArrays('SELECT text FROM files WHERE path = ?', [path]);
        return rows.length ? rows[0][0] : undefined;
      } catch (err) {
        throw words(err);
      }
    },
    memReplace(path, text) {
      try {
        memory.exec({ sql: 'INSERT OR REPLACE INTO files (path, text) VALUES (?, ?)', bind: [path, text] });
      } catch (err) {
        throw words(err);
      }
    },
    memAppend(path, text) {
      try {
        memory.exec({
          sql: 'INSERT INTO files (path, text) VALUES (?, ?) ON CONFLICT (path) DO UPDATE SET text = text || excluded.text',
          bind: [path, text],
        });
      } catch (err) {
        throw words(err);
      }
    },
    memRemove(path) {
      try {
        memory.exec({ sql: 'DELETE FROM files WHERE path = ?', bind: [path] });
      } catch (err) {
        throw words(err);
      }
    },
  };
}

// The tab as the engine reads it.
const named = () => JSON.stringify({ db: tab.name, endpoint: tab.endpoint, ignore: tab.ignore, maxWire: tab.maxWire });

const settings = (multi) =>
  JSON.stringify({ ...multi, now: multi.now ?? Math.floor(Date.now() / 1000) });

// Runs `body` alone among every tab of the origin, then fires table-changed
// for each table it wrote rows into, and returns its record.
async function alone(body) {
  const answer = await navigator.locks.request(`smugglr:${tab.name}`, () => body());
  if (answer === undefined || answer === null) {
    return null;
  }
  const { record, changed } = JSON.parse(answer);
  for (const [table, keys] of Object.entries(changed)) {
    channel.postMessage({ type: 'table-changed', table, keys });
  }
  return record;
}

const ops = {
  async open(options, urls) {
    tab = { headers: {}, ignore: [], ...options };
    const { default: init } = await import(urls.sqlite);
    engine = await import(urls.engine);
    await engine.default();
    const sqlite3 = await init();
    globalThis.smugglrHost = host();
    app = new sqlite3.oo1.OpfsDb(`/${tab.name}`);
    app.createFunction({
      name: 'smugglr_row_hash',
      xFunc: (_context, ...columns) => engine.rowHash(columns),
      arity: -1,
      deterministic: true,
    });
    memory = new sqlite3.oo1.OpfsDb(`/${tab.name}.smugglr`);
    memory.exec('CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, text TEXT NOT NULL)');
    channel = new BroadcastChannel(`smugglr:${tab.name}`);
    if (tab.schema) {
      app.exec(tab.schema);
    }
    return alone(() => engine.openTab(named()));
  },
  query(sql, params) {
    return run(app, { sql, params }).rows;
  },
  exec(sql, params) {
    return run(app, { sql, params }).rowsAffected;
  },
  sync() {
    return alone(() => engine.syncTab(named()));
  },
  stash(multi) {
    return navigator.locks.request(`smugglr:${tab.name}`, () => engine.stashTab(named(), settings(multi)));
  },
  retrieve(blobs, multi) {
    return alone(() => engine.retrieveTab(named(), settings(multi), blobs));
  },
  replay(change, multi) {
    return alone(() => engine.replayTab(named(), settings(multi), change));
  },
  close() {
    channel?.close();
    app?.close();
    memory?.close();
    return null;
  },
};

self.onmessage = async ({ data: { id, op, args } }) => {
  try {
    const value = await ops[op](...args);
    self.postMessage({ id, ok: true, value });
  } catch (err) {
    self.postMessage({ id, ok: false, error: words(err) });
  }
};
