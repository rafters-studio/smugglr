// Web Worker host: owns wa-sqlite + OPFS + smugglr. Main thread proxies
// RPC calls in via postMessage. SyncAccessHandle is worker-only in WebKit
// and Firefox and is the spec-correct context for OPFS-backed SQLite.

import SQLiteAsyncESMFactory from "wa-sqlite/dist/wa-sqlite-async.mjs";
import * as SQLite from "wa-sqlite";
// @ts-expect-error - wa-sqlite ships JS examples without .d.ts
import { OriginPrivateFileSystemVFS } from "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js";

import { Smugglr, createWaSqliteExecutor, setWasm } from "../dist/index.js";
import { startAutoSync, type AutoSyncTarget } from "../dist/autoSync.js";
import * as wasm from "../dist/wasm/smugglr_wasm.js";

let sqlite3: any = null;
let db: number | null = null;

async function init(dbPath: string) {
  await setWasm(wasm as never);
  const module = await SQLiteAsyncESMFactory();
  sqlite3 = SQLite.Factory(module);
  const vfs = new OriginPrivateFileSystemVFS();
  await new Promise((r) => setTimeout(r, 0));
  sqlite3.vfs_register(vfs, true);
  db = await sqlite3.open_v2(
    dbPath,
    SQLite.SQLITE_OPEN_READWRITE | SQLite.SQLITE_OPEN_CREATE,
    "opfs",
  );
}

async function runSql(sql: string, params: unknown[]) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const exe = createWaSqliteExecutor(sqlite3, db);
  return exe.run(sql, params);
}

async function sync(opts: {
  destUrl: string;
  tables: string[];
  conflict?: "local_wins" | "remote_wins" | "newer_wins" | "uuid_v7_wins";
  direction?: "push" | "pull" | "sync";
  /** When set, subscribes to "table-changed" before sync and returns captured events. */
  captureEvents?: boolean;
  /** When set, also test that the unsubscribe function silences subsequent events. */
  testUnsubscribe?: boolean;
}) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: {
      tables: opts.tables,
      conflictResolution: opts.conflict ?? "newer_wins",
    },
  });

  const events: unknown[] = [];
  let unsub: (() => void) | null = null;
  if (opts.captureEvents) {
    unsub = s.on("table-changed", (e) => events.push(e));
  }

  const dir = opts.direction ?? "sync";
  const result = dir === "push" ? await s.push()
    : dir === "pull" ? await s.pull()
    : await s.sync();

  let postUnsubEvents: unknown[] = [];
  if (opts.testUnsubscribe && unsub) {
    unsub();
    const after: unknown[] = [];
    s.on("table-changed", (e) => after.push(e));
    // Trigger a no-op pull -- since the rows already match, no event should fire.
    await s.pull();
    postUnsubEvents = after;
  }

  s.dispose();
  if (!opts.captureEvents) return result;
  return { result, events, postUnsubEvents };
}

async function eraseLocal(opts: { destUrl: string; tables: string[] }) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: { tables: opts.tables },
  });
  const result = await s.eraseLocal();
  s.dispose();
  return result;
}

async function syncWithAuthSwap(opts: {
  destUrl: string;
  initialToken: string;
  newToken: string;
  tables: string[];
}) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, authToken: opts.initialToken, profile: "generic" },
    sync: { tables: opts.tables },
  });

  const r1 = await s.push();
  s.updateAuth(opts.newToken);
  const r2 = await s.push();

  s.dispose();
  return { firstPush: r1, secondPush: r2 };
}

async function anonymousMode() {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    sync: { tables: ["users"] },
  });

  const diff = await s.diff();
  let pushError: string | null = null;
  try {
    await s.push();
  } catch (e) {
    pushError = e instanceof Error ? e.message : String(e);
  }

  s.dispose();
  return { diff, pushError };
}

// Runs Smugglr.init with autoSync configured, optionally fires a synthetic
// `online` event, and returns the captured row count + the request fingerprint
// the mock saw (so the test can assert "did a pull actually happen").
async function autoSync(opts: {
  destUrl: string;
  tables: string[];
  onInit?: "hydrate-if-empty" | "always" | "never";
  triggerOnline?: boolean;
}) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: { tables: opts.tables },
    autoSync: { onInit: opts.onInit ?? "hydrate-if-empty", onReconnect: true },
  });

  if (opts.triggerOnline) {
    // online -> 250ms debounce -> lock + mocked round-trip. 750ms covers all three.
    self.dispatchEvent(new Event("online"));
    await new Promise((r) => setTimeout(r, 750));
  }

  const local = await createWaSqliteExecutor(sqlite3, db).run(
    `SELECT id, name, updated_at FROM ${opts.tables[0]} ORDER BY id`,
    [],
  );

  s.stopAutoSync();
  s.dispose();
  return { local };
}

// Regression for #210: an unsubscribe handle returned by on() must be safe to
// invoke AFTER dispose()/free(). Before the fix the closure dereferenced freed
// WASM memory; the guarded wrapper makes it an inert no-op. We also assert
// on() itself rejects after dispose. Returns flags the test asserts on.
async function unsubAfterDispose(opts: { destUrl: string; tables: string[] }) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: { tables: opts.tables },
  });

  const unsub = s.on("table-changed", () => {});
  s.dispose();

  // Invoking the stale handle after free() must not fault. Before the fix this
  // dereferenced a dangling *const Smugglr in wasm.
  let unsubThrew = false;
  try {
    unsub();
    unsub(); // double-invoke is also a no-op
  } catch {
    unsubThrew = true;
  }

  // on() after dispose must surface a CLEAN guard error BEFORE touching freed
  // wasm memory. Use-after-free is non-deterministic (wasm may silently read
  // stale bytes), so the deterministic signal of the fix is this specific
  // guard message -- which only the JS wrapper, not the freed Rust side, emits.
  let onAfterDisposeError = "";
  try {
    s.on("table-changed", () => {});
  } catch (e) {
    onAfterDisposeError = e instanceof Error ? e.message : String(e);
  }

  return { unsubThrew, onAfterDisposeError };
}

// Regression for #207: after only calling setWasm() (no manual mod.default()),
// the module must be fully instantiated so Smugglr.init() works. The worker's
// init() above already relies on this; this op makes it an explicit assertion
// by exercising a real sync round-trip with a module that was set, not
// hand-initialized. Returns the sync status.
async function setWasmInitsModule(opts: { destUrl: string; tables: string[] }) {
  if (!sqlite3 || db === null) throw new Error("init() first");
  const s = await Smugglr.init({
    source: { type: "local", executor: createWaSqliteExecutor(sqlite3, db) },
    dest: { url: opts.destUrl, profile: "generic" },
    sync: { tables: opts.tables },
  });
  const result = (await s.diff()) as { command: string; status: string };
  s.dispose();
  return result;
}

// Regression for #208: a transient WASM load failure must NOT permanently brick
// init(). loadWasm caches the in-flight promise in `wasmReady`; before the fix a
// rejected load stuck around forever, so every later init() re-threw the stale
// rejection. The fix clears wasmReady/wasmModule on failure so a retry works.
//
// We drive Smugglr.init(config, { wasmModule }) where the injected module's
// default() rejects on the first call and resolves on the second. With the fix
// the second init() succeeds; without it the second init() re-throws the cached
// failure. This op MUST run in a fresh worker before any other init() has set
// wasmReady, so its test does not call window.e2e.init().
async function loadWasmTransientRetry() {
  let calls = 0;
  // A minimal wasm-bindgen-shaped module. default() fails first, succeeds after.
  const fakeModule = {
    default: () => {
      calls += 1;
      return calls === 1
        ? Promise.reject(new Error("transient .wasm fetch 503"))
        : Promise.resolve(undefined);
    },
    // Smugglr.init returns a stub inner we never call methods on; the test only
    // asserts the second init() resolves rather than re-throwing the cached err.
    Smugglr: {
      init: () => ({
        free() {},
        push: () => Promise.resolve({}),
        pull: () => Promise.resolve({}),
        sync: () => Promise.resolve({}),
        diff: () => Promise.resolve({}),
        on: () => () => {},
        eraseLocal: () => Promise.resolve({}),
        updateAuth() {},
        updateDest() {},
      }),
    },
  };

  const config = {
    source: { url: "https://src.smugglr.test", profile: "generic" },
    dest: { url: "https://dest.smugglr.test", profile: "generic" },
    sync: { tables: ["users"] },
  };

  let firstFailed = false;
  try {
    await Smugglr.init(config as never, { wasmModule: fakeModule as never });
  } catch {
    firstFailed = true;
  }

  // The retry: if the cache was not cleared, this re-throws the same error.
  let secondSucceeded = false;
  try {
    const s = await Smugglr.init(config as never, { wasmModule: fakeModule as never });
    s.dispose();
    secondSucceeded = true;
  } catch {
    secondSucceeded = false;
  }

  return { firstFailed, secondSucceeded, defaultCalls: calls };
}

// Regression for #209: the init-pull retry loop and the online-sync retry loop
// must use INDEPENDENT attempt counters. Before the fix they shared one
// module-scoped `attempt`, so a continuously-failing init pull inflated the
// online-sync's exponential backoff: sync's FIRST retry used a delay of
// initialMs * 2^(pull's accumulated attempts) instead of initialMs * 2^0.
//
// Setup: pull always rejects (its loop keeps incrementing the shared counter
// every ~initialMs). sync always rejects too, and we timestamp each sync
// attempt. We measure the gap between sync attempt #1 and #2:
//   - fixed (independent counters): sync's own attempt starts at 0 -> gap ~=
//     initialMs (50ms).
//   - buggy (shared counter): by the time online fires the pull loop has
//     advanced the counter several times, so the gap balloons toward maxMs.
//
// jitter:false makes the delay deterministic. We assert the gap is small.
async function autoSyncRetryIsolation() {
  const syncTimes: number[] = [];
  let sawPull = false;
  const target: AutoSyncTarget = {
    pull: () => { sawPull = true; return Promise.reject(new Error("boom")); },
    sync: () => { syncTimes.push(performance.now()); return Promise.reject(new Error("boom")); },
  };

  const runtime = startAutoSync({
    target,
    config: {
      onInit: "always",
      onReconnect: true,
      // initialMs small; maxMs large so a corrupted (shared) counter produces a
      // visibly large first-retry gap. jitter off for determinism.
      backoff: { initialMs: 50, maxMs: 4000, jitter: false },
    },
    // Non-local dest so the locks/online wiring engages; the URLs are never
    // fetched because the fake target rejects before any network call.
    source: { url: "https://src.smugglr.test", profile: "generic" },
    dest: { url: "https://dest.smugglr.test", profile: "generic" },
    sync: { tables: ["users"] },
  });

  // Give the init "always" pull loop time to fail and re-fire several times,
  // advancing the (shared, under the bug) attempt counter well past 0.
  await new Promise((r) => setTimeout(r, 300));
  // Now fire online -> 250ms debounce -> first sync attempt -> schedule retry.
  self.dispatchEvent(new Event("online"));
  // Wait long enough to observe a fixed-backoff (~50ms) second sync attempt,
  // but NOT long enough to observe a buggy (inflated, >=800ms) one within a
  // tight bound. 250 debounce + 600 margin.
  await new Promise((r) => setTimeout(r, 850));

  runtime.stop();
  await new Promise((r) => setTimeout(r, 50));

  // Gap between the first two sync attempts. Under the fix this is ~initialMs.
  const firstSyncGap = syncTimes.length >= 2 ? syncTimes[1] - syncTimes[0] : -1;

  return {
    sawPull,
    sawSync: syncTimes.length > 0,
    syncAttempts: syncTimes.length,
    firstSyncGap,
  };
}

async function reset() {
  if (sqlite3 && db !== null) {
    sqlite3.close(db);
    db = null;
  }
  const root = await navigator.storage.getDirectory();
  for await (const entry of (root as any).values()) {
    await root.removeEntry(entry.name, { recursive: true });
  }
}

interface RpcCall {
  id: number;
  op:
    | "init"
    | "runSql"
    | "sync"
    | "eraseLocal"
    | "syncWithAuthSwap"
    | "anonymousMode"
    | "autoSync"
    | "autoSyncRetryIsolation"
    | "unsubAfterDispose"
    | "setWasmInitsModule"
    | "loadWasmTransientRetry"
    | "reset";
  args: unknown[];
}

self.addEventListener("message", async (ev: MessageEvent<RpcCall>) => {
  const { id, op, args } = ev.data;
  try {
    let result: unknown;
    switch (op) {
      case "init": result = await init(args[0] as string); break;
      case "runSql": result = await runSql(args[0] as string, (args[1] as unknown[]) ?? []); break;
      case "sync": result = await sync(args[0] as Parameters<typeof sync>[0]); break;
      case "eraseLocal": result = await eraseLocal(args[0] as Parameters<typeof eraseLocal>[0]); break;
      case "syncWithAuthSwap": result = await syncWithAuthSwap(args[0] as Parameters<typeof syncWithAuthSwap>[0]); break;
      case "anonymousMode": result = await anonymousMode(); break;
      case "autoSync": result = await autoSync(args[0] as Parameters<typeof autoSync>[0]); break;
      case "autoSyncRetryIsolation": result = await autoSyncRetryIsolation(); break;
      case "unsubAfterDispose": result = await unsubAfterDispose(args[0] as Parameters<typeof unsubAfterDispose>[0]); break;
      case "setWasmInitsModule": result = await setWasmInitsModule(args[0] as Parameters<typeof setWasmInitsModule>[0]); break;
      case "loadWasmTransientRetry": result = await loadWasmTransientRetry(); break;
      case "reset": result = await reset(); break;
    }
    (self as unknown as Worker).postMessage({ id, ok: true, result });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    (self as unknown as Worker).postMessage({ id, ok: false, error: message });
  }
});
