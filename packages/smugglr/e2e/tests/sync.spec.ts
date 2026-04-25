// e2e: real OPFS-backed wa-sqlite database synced with smugglr against a
// generic HTTP SQL target mocked via Playwright route().
//
// Generic profile request: POST { sql, params? } -> { columns, rows }.

import { expect, test, type Page, type Route } from "@playwright/test";

interface RemoteRow {
  id: string;
  name: string;
  updated_at: number;
  hash?: string;
}

interface MockState {
  tableExists: boolean;
  rows: Map<string, RemoteRow>;
  requests: Array<{ sql: string; params: unknown[] }>;
}

function freshState(): MockState {
  return { tableExists: true, rows: new Map(), requests: [] };
}

function reply(route: Route, columns: string[], rows: unknown[][]) {
  return route.fulfill({
    status: 200,
    contentType: "application/json",
    body: JSON.stringify({ columns, rows }),
  });
}

function emptyOk(route: Route) {
  return reply(route, [], []);
}

function installMockTarget(page: Page, state: MockState, host = "https://mock.smugglr.test") {
  return page.route(`${host}/**`, async (route) => {
    const body = JSON.parse(route.request().postData() ?? "{}") as {
      sql: string;
      params?: unknown[];
    };
    const sql = body.sql.trim();
    const params = body.params ?? [];
    state.requests.push({ sql, params });

    const lower = sql.toLowerCase();

    // Schema introspection (sqlite_master / pragma).
    if (lower.startsWith("select name from sqlite_master")) {
      return reply(route, ["name"], state.tableExists ? [["users"]] : []);
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

    // Reads against the users table. smugglr issues two read shapes:
    //   metadata: SELECT *, CAST("id" AS TEXT) AS __pk FROM "users"
    //   row fetch: SELECT * FROM "users" WHERE CAST("id" AS TEXT) IN (?, ...)
    if (lower.startsWith("select") && lower.includes('from "users"')) {
      const wantsPk = lower.includes("__pk");
      const cols = wantsPk
        ? ["id", "name", "updated_at", "__pk"]
        : ["id", "name", "updated_at"];
      const matchesId = (id: string) => {
        if (!lower.includes(" in (")) return true;
        return params.some((p) => String(p) === id);
      };
      const out: unknown[][] = [];
      for (const r of state.rows.values()) {
        if (!matchesId(r.id)) continue;
        const row: unknown[] = [r.id, r.name, r.updated_at];
        if (wantsPk) row.push(r.id);
        out.push(row);
      }
      return reply(route, cols, out);
    }

    if (lower.startsWith("insert") || lower.startsWith("replace")) {
      // Parse the column list from the statement so we can map flat params
      // back to named fields regardless of column order.
      const colMatch = sql.match(/\(([^)]+)\)\s+VALUES/i);
      const cols = colMatch
        ? colMatch[1].split(",").map((c) => c.trim().replace(/^"|"$/g, ""))
        : ["id", "name", "updated_at"];
      const width = cols.length;
      for (let i = 0; i + width - 1 < params.length; i += width) {
        const row: Record<string, unknown> = {};
        for (let c = 0; c < width; c++) {
          row[cols[c]] = params[i + c];
        }
        const id = String(row.id ?? "");
        state.rows.set(id, {
          id,
          name: String(row.name ?? ""),
          updated_at: Number(row.updated_at ?? 0),
        });
      }
      return emptyOk(route);
    }

    if (lower.startsWith("delete")) {
      return emptyOk(route);
    }

    if (lower.startsWith("create table") || lower.startsWith("create index")) {
      return emptyOk(route);
    }

    return emptyOk(route);
  });
}

async function bootstrap(page: Page) {
  await page.goto("/");
  await expect(page.locator("#status")).toHaveText("ready");
  await page.evaluate(() => window.e2e.reset());
  await page.evaluate(() => window.e2e.init("e2e.db"));
}

test.describe("smugglr OPFS e2e", () => {
  test.beforeEach(async ({ page }) => {
    await bootstrap(page);
  });

  test("push: local OPFS rows reach the mock HTTP target", async ({ page }) => {
    const state = freshState();
    await installMockTarget(page, state);

    await page.evaluate(() =>
      window.e2e.runSql(
        "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER)",
      ),
    );
    await page.evaluate(() =>
      window.e2e.runSql(
        "INSERT INTO users (id, name, updated_at) VALUES (?, ?, ?), (?, ?, ?)",
        ["u1", "ada", 100, "u2", "lin", 200],
      ),
    );

    const result = await page.evaluate(() =>
      window.e2e.sync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        direction: "push",
      }),
    );

    expect(result).toMatchObject({ command: "push", status: "ok" });
    expect(state.requests.length).toBeGreaterThan(0);
    expect(state.rows.size).toBe(2);
    expect(state.rows.get("u1")?.name).toBe("ada");
    expect(state.rows.get("u2")?.name).toBe("lin");
  });

  test("pull: remote rows land in local OPFS", async ({ page }) => {
    const state = freshState();
    state.rows.set("r1", { id: "r1", name: "remote-only", updated_at: 500 });
    await installMockTarget(page, state);

    await page.evaluate(() =>
      window.e2e.runSql(
        "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER)",
      ),
    );

    const result = await page.evaluate(() =>
      window.e2e.sync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        direction: "pull",
      }),
    );

    expect(result).toMatchObject({ command: "pull", status: "ok" });

    const local = (await page.evaluate(() =>
      window.e2e.runSql("SELECT id, name, updated_at FROM users ORDER BY id"),
    )) as { columns: string[]; rows: unknown[][] };

    expect(local.rows).toEqual([["r1", "remote-only", 500]]);
  });

  test("on('table-changed'): fires once per pulled table with the right pks", async ({ page }) => {
    const state = freshState();
    state.rows.set("r1", { id: "r1", name: "alpha", updated_at: 100 });
    state.rows.set("r2", { id: "r2", name: "beta", updated_at: 200 });
    await installMockTarget(page, state);

    await page.evaluate(() =>
      window.e2e.runSql(
        "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT, updated_at INTEGER)",
      ),
    );

    const out = (await page.evaluate(() =>
      window.e2e.sync({
        destUrl: "https://mock.smugglr.test",
        tables: ["users"],
        direction: "pull",
        captureEvents: true,
        testUnsubscribe: true,
      }),
    )) as {
      result: { command: string; status: string };
      events: Array<{ table: string; changedPks: string[]; removedPks: string[]; source: string }>;
      postUnsubEvents: unknown[];
    };

    expect(out.result).toMatchObject({ command: "pull", status: "ok" });
    expect(out.events).toHaveLength(1);
    expect(out.events[0].table).toBe("users");
    expect(out.events[0].source).toBe("pull");
    expect(out.events[0].removedPks).toEqual([]);
    expect(out.events[0].changedPks.sort()).toEqual(["r1", "r2"]);

    // Unsubscribed listener should be silent; second listener that registered
    // after unsubscribe sees zero events because the second pull had no diff.
    expect(out.postUnsubEvents).toEqual([]);
  });
});
