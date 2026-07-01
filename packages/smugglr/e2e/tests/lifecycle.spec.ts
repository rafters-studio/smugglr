// e2e: WASM lifecycle + autoSync retry-isolation regressions.
//
// Covers audit fixes:
//   #207 setWasm() must instantiate the WASM module (mod.default()).
//   #208 loadWasm must not cache a rejected promise forever (transient retry).
//   #209 init-pull and online-sync retry loops must back off / cancel independently.
//   #210 unsubscribe handle returned by on() must be inert after dispose()/free().

import { expect, test, type Page, type Route } from "@playwright/test";

interface RemoteRow { id: string; name: string; updated_at: number }

interface MockState {
  rows: Map<string, RemoteRow>;
}

function reply(route: Route, columns: string[], rows: unknown[][]) {
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ columns, rows }),
  });
}

function installMockTarget(page: Page, state: MockState, host = "https://mock.smugglr.test") {
  return page.route(`${host}/**`, async (route) => {
    const body = JSON.parse(route.request().postData() ?? "{}") as {
      sql: string; params?: unknown[];
    };
    const lower = body.sql.trim().toLowerCase();
    if (lower.startsWith("select name from sqlite_master")) {
      return reply(route, ["name"], [["users"]]);
    }
    if (lower.startsWith("pragma table_info")) {
      return reply(
        route,
        ["cid", "name", "type", "notnull", "dflt_value", "pk"],
        [
          [0, "id", "TEXT", 1, null, 1],
          [1, "name", "TEXT", 0, null, 0],
          [2, "updated_at", "INTEGER", 0, null, 0],
        ],
      );
    }
    if (lower.startsWith("select") && lower.includes('from "users"')) {
      const out: unknown[][] = [];
      for (const r of state.rows.values()) out.push([r.id, r.name, r.updated_at, r.id]);
      const cols = lower.includes("__pk")
        ? ["id", "name", "updated_at", "__pk"]
        : ["id", "name", "updated_at"];
      return reply(route, cols, lower.includes("__pk") ? out : out.map((r) => r.slice(0, 3)));
    }
    return reply(route, [], []);
  });
}

async function bootstrap(page: Page) {
  await page.goto("/");
  await expect(page.locator("#status")).toHaveText("ready");
  await page.evaluate(() => window.e2e.reset());
  await page.evaluate(() => window.e2e.init("lifecycle.db"));
  await page.evaluate(() =>
    window.e2e.runSql(
      "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER)",
    ),
  );
}

test.describe("lifecycle", () => {
  test.beforeEach(async ({ page }) => { await bootstrap(page); });

  // #207: the worker's init() now relies solely on `await setWasm(wasm)` to
  // instantiate the module (no hand-call to wasm.default()). If setWasm did not
  // run mod.default(), Smugglr.init() would fault against an uninitialized
  // module. A successful diff round-trip proves setWasm initialized the WASM.
  test("#207 setWasm() initializes the WASM module so init/diff works", async ({ page }) => {
    const state: MockState = { rows: new Map() };
    await installMockTarget(page, state);

    const out = (await page.evaluate(() =>
      window.e2e.setWasmInitsModule({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
      }),
    )) as { command: string; status: string };

    expect(out).toMatchObject({ command: "diff", status: "ok" });
  });

  // #210: a stale unsubscribe handle invoked after dispose() must be a no-op,
  // not a use-after-free deref of the freed Rust Smugglr. on() after dispose
  // must throw loudly rather than reach freed memory.
  test("#210 unsubscribe after dispose() is an inert no-op (no use-after-free)", async ({ page }) => {
    const state: MockState = { rows: new Map() };
    await installMockTarget(page, state);

    const out = (await page.evaluate(() =>
      window.e2e.unsubAfterDispose({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
      }),
    )) as { unsubThrew: boolean; onAfterDisposeError: string };

    // Calling the stale unsubscribe handle (twice) must NOT throw/fault.
    expect(out.unsubThrew).toBe(false);
    // Subscribing after dispose must surface the wrapper's clean guard error,
    // emitted by the JS wrapper BEFORE it ever calls into the freed Rust
    // instance. Before the fix on() reached `this.inner.on()` on freed memory
    // (undefined behavior), never producing this deterministic message.
    expect(out.onAfterDisposeError).toContain("Smugglr.on() called after dispose()");
  });

  // #209: the init-pull retry loop and the online-sync retry loop must use
  // independent attempt counters. Before the fix they shared one counter, so a
  // continuously-failing init pull inflated the online-sync's first retry delay
  // from ~initialMs (50ms) toward maxMs. We measure the gap between the first
  // two sync attempts: under the fix it is ~initialMs; under the bug it balloons.
  test("#209 init-pull and online-sync retry loops back off independently", async ({ page }) => {
    const out = (await page.evaluate(() =>
      window.e2e.autoSyncRetryIsolation(),
    )) as { sawPull: boolean; sawSync: boolean; syncAttempts: number; firstSyncGap: number };

    // Both loops actually ran (and failed, entering retry).
    expect(out.sawPull).toBe(true);
    expect(out.sawSync).toBe(true);
    // The sync loop retried (so we have a measurable first-to-second gap).
    expect(out.syncAttempts).toBeGreaterThanOrEqual(2);
    // With independent counters sync's first retry uses initialMs (50ms), not
    // the pull-inflated delay. Allow generous slack for scheduler jitter but
    // far below the buggy >=800ms a shared counter would produce.
    expect(out.firstSyncGap).toBeGreaterThan(0);
    expect(out.firstSyncGap).toBeLessThan(400);
  });
});

// Separate describe with NO init() in setup: loadWasmTransientRetry must run in
// a fresh worker where `wasmReady` is still null, so it exercises loadWasm's
// real caching path (not the setWasm short-circuit the other tests rely on).
test.describe("lifecycle (fresh module state)", () => {
  // #208: a transient WASM load failure must not permanently brick init().
  test("#208 a transient WASM load failure does not brick later init() calls", async ({ page }) => {
    await page.goto("/");
    await expect(page.locator("#status")).toHaveText("ready");
    await page.evaluate(() => window.e2e.reset());

    const out = (await page.evaluate(() =>
      window.e2e.loadWasmTransientRetry(),
    )) as { firstFailed: boolean; secondSucceeded: boolean; defaultCalls: number };

    // First init() saw the transient default() rejection.
    expect(out.firstFailed).toBe(true);
    // Retry: the cached rejected promise was cleared, so the second init()
    // re-ran default() and succeeded. Before the fix this stayed false (the
    // stale rejection was re-thrown) and defaultCalls would be 1, not 2.
    expect(out.secondSucceeded).toBe(true);
    expect(out.defaultCalls).toBe(2);
  });
});
