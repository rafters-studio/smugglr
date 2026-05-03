// e2e: autoSync (empty-state hydration + reconnect-triggered sync) against
// the same generic-profile mock the sync.spec uses.

import { expect, test, type Page, type Route } from "@playwright/test";

interface RemoteRow { id: string; name: string; updated_at: number }

interface MockState {
  rows: Map<string, RemoteRow>;
  requests: Array<{ sql: string; params: unknown[] }>;
}

function freshState(): MockState {
  return { rows: new Map(), requests: [] };
}

function reply(route: Route, columns: string[], rows: unknown[][]) {
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ columns, rows }),
  });
}

function emptyOk(route: Route) { return reply(route, [], []); }

function installMockTarget(page: Page, state: MockState, host = "https://mock.smugglr.test") {
  return page.route(`${host}/**`, async (route) => {
    const body = JSON.parse(route.request().postData() ?? "{}") as {
      sql: string; params?: unknown[];
    };
    const sql = body.sql.trim();
    const params = body.params ?? [];
    state.requests.push({ sql, params });
    const lower = sql.toLowerCase();

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
      const wantsPk = lower.includes("__pk");
      const cols = wantsPk
        ? ["id", "name", "updated_at", "__pk"]
        : ["id", "name", "updated_at"];
      const matchesId = (id: string) =>
        !lower.includes(" in (") || params.some((p) => String(p) === id);
      const out: unknown[][] = [];
      for (const r of state.rows.values()) {
        if (!matchesId(r.id)) continue;
        const row: unknown[] = [r.id, r.name, r.updated_at];
        if (wantsPk) row.push(r.id);
        out.push(row);
      }
      return reply(route, cols, out);
    }
    return emptyOk(route);
  });
}

async function bootstrap(page: Page) {
  await page.goto("/");
  await expect(page.locator("#status")).toHaveText("ready");
  await page.evaluate(() => window.e2e.reset());
  await page.evaluate(() => window.e2e.init("auto-sync.db"));
  await page.evaluate(() =>
    window.e2e.runSql(
      "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER)",
    ),
  );
}

test.describe("autoSync", () => {
  test.beforeEach(async ({ page }) => { await bootstrap(page); });

  test("hydrate-if-empty: empty local + populated remote -> auto-pulls on init", async ({ page }) => {
    const state = freshState();
    state.rows.set("r1", { id: "r1", name: "ada", updated_at: 100 });
    state.rows.set("r2", { id: "r2", name: "lin", updated_at: 200 });
    await installMockTarget(page, state);

    const out = (await page.evaluate(() =>
      window.e2e.autoSync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        onInit: "hydrate-if-empty",
      }),
    )) as { local: { rows: unknown[][] } };

    expect(out.local.rows).toEqual([
      ["r1", "ada", 100],
      ["r2", "lin", 200],
    ]);
  });

  test("hydrate-if-empty: pre-populated local skips the pull", async ({ page }) => {
    const state = freshState();
    state.rows.set("r1", { id: "r1", name: "remote-only", updated_at: 999 });
    await installMockTarget(page, state);

    await page.evaluate(() =>
      window.e2e.runSql(
        "INSERT INTO users (id, name, updated_at) VALUES (?, ?, ?)",
        ["local1", "already-here", 1],
      ),
    );

    const out = (await page.evaluate(() =>
      window.e2e.autoSync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        onInit: "hydrate-if-empty",
      }),
    )) as { local: { rows: unknown[][] } };

    // Local is untouched; the remote-only row never landed because empty
    // detection bailed before pull.
    expect(out.local.rows).toEqual([["local1", "already-here", 1]]);
    // No request to the mock should have happened either.
    expect(state.requests.length).toBe(0);
  });

  test("onReconnect: synthetic online event triggers a sync against the dest", async ({ page }) => {
    const state = freshState();
    state.rows.set("r1", { id: "r1", name: "ada", updated_at: 100 });
    await installMockTarget(page, state);

    // onInit: never -- we want to isolate the online-triggered path.
    const out = (await page.evaluate(() =>
      window.e2e.autoSync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        onInit: "never",
        triggerOnline: true,
      }),
    )) as { local: { rows: unknown[][] } };

    // Reconnect ran sync, so the remote row should have landed locally.
    expect(out.local.rows).toEqual([["r1", "ada", 100]]);
    expect(state.requests.length).toBeGreaterThan(0);
  });
});
