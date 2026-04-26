import { afterEach, describe, expect, it, vi } from "vitest";
import { atom, map } from "nanostores";
import type { SqlExecutor, TableChangedEvent } from "smugglr";

import { smuggl } from "./index.js";

interface Row {
  key: string;
  value: string;
  updated_at: string;
}

function fixtureExecutor(initial: Row[] = []) {
  const rows = new Map<string, Row>();
  for (const r of initial) rows.set(r.key, r);

  const executor: SqlExecutor = {
    async run(sql, params) {
      const lower = sql.trim().toLowerCase();
      if (lower.startsWith("select value")) {
        const r = rows.get(String(params[0]));
        return { columns: ["value"], rows: r ? [[r.value]] : [] };
      }
      if (lower.startsWith("insert")) {
        const [key, value, updated_at] = params as string[];
        rows.set(key, { key, value, updated_at });
        return { columns: [], rows: [] };
      }
      throw new Error(`fixture executor: unknown SQL: ${sql}`);
    },
  };
  return { executor, rows };
}

function fixtureSmugglr() {
  const handlers: Array<(e: TableChangedEvent) => void> = [];
  return {
    on(_event: "table-changed", handler: (e: TableChangedEvent) => void) {
      handlers.push(handler);
      return () => {
        const idx = handlers.indexOf(handler);
        if (idx >= 0) handlers.splice(idx, 1);
      };
    },
    fire(event: TableChangedEvent) {
      for (const h of handlers) h(event);
    },
  };
}

const flushMicrotasks = async () => {
  // Multiple awaits so chains of `await executor.run(...)` -> set() complete.
  for (let i = 0; i < 5; i++) await new Promise((r) => setTimeout(r, 0));
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("@smugglr/nanostores", () => {
  it("hydrates an atom from existing row", async () => {
    const { executor } = fixtureExecutor([
      { key: "todos", value: JSON.stringify(["alpha", "beta"]), updated_at: "x" },
    ]);
    const smugglr = fixtureSmugglr();
    const $todos = atom<string[]>([]);

    smuggl($todos, { smugglr, executor, table: "app_state", key: "todos" });

    await flushMicrotasks();
    expect($todos.get()).toEqual(["alpha", "beta"]);
  });

  it("persists on every atom change", async () => {
    const { executor, rows } = fixtureExecutor();
    const smugglr = fixtureSmugglr();
    const $count = atom(0);

    smuggl($count, { smugglr, executor, table: "app_state", key: "count" });

    await flushMicrotasks();
    $count.set(1);
    $count.set(2);
    await flushMicrotasks();

    const stored = rows.get("count");
    expect(stored).toBeDefined();
    expect(JSON.parse(stored!.value)).toBe(2);
  });

  it("works with map stores", async () => {
    const { executor, rows } = fixtureExecutor();
    const smugglr = fixtureSmugglr();
    const $user = map<{ name: string; email: string }>({ name: "", email: "" });

    smuggl($user, { smugglr, executor, table: "app_state", key: "user" });

    await flushMicrotasks();
    $user.setKey("name", "ada");
    await flushMicrotasks();

    const stored = rows.get("user");
    expect(stored).toBeDefined();
    expect(JSON.parse(stored!.value)).toEqual({ name: "ada", email: "" });
  });

  it("re-hydrates on matching table-changed event", async () => {
    const { executor, rows } = fixtureExecutor([
      { key: "todos", value: JSON.stringify([]), updated_at: "t0" },
    ]);
    const smugglr = fixtureSmugglr();
    const $todos = atom<string[]>([]);

    smuggl($todos, { smugglr, executor, table: "app_state", key: "todos" });

    await flushMicrotasks();
    expect($todos.get()).toEqual([]);

    rows.set("todos", { key: "todos", value: JSON.stringify(["from-remote"]), updated_at: "t1" });
    smugglr.fire({
      table: "app_state",
      changedPks: ["todos"],
      removedPks: [],
      source: "pull",
    });

    await flushMicrotasks();
    expect($todos.get()).toEqual(["from-remote"]);
  });

  it("ignores events for other tables / other keys", async () => {
    const { executor } = fixtureExecutor([
      { key: "todos", value: JSON.stringify([]), updated_at: "t0" },
    ]);
    const smugglr = fixtureSmugglr();
    const onHydrate = vi.fn();
    const $todos = atom<string[]>([]);

    smuggl($todos, {
      smugglr,
      executor,
      table: "app_state",
      key: "todos",
      onHydrate,
    });

    await flushMicrotasks();
    expect(onHydrate).toHaveBeenCalledTimes(1);

    smugglr.fire({ table: "other", changedPks: ["todos"], removedPks: [], source: "pull" });
    smugglr.fire({ table: "app_state", changedPks: ["other"], removedPks: [], source: "pull" });

    await flushMicrotasks();
    expect(onHydrate).toHaveBeenCalledTimes(1);
  });

  it("returned dispose function detaches both listeners", async () => {
    const { executor, rows } = fixtureExecutor();
    const smugglr = fixtureSmugglr();
    const $count = atom(0);

    const dispose = smuggl($count, {
      smugglr,
      executor,
      table: "app_state",
      key: "count",
    });

    await flushMicrotasks();
    dispose();

    $count.set(99);
    await flushMicrotasks();

    // Persist listener detached: row stays at the seeded 0 (or absent if no initial write happened yet).
    const stored = rows.get("count");
    if (stored) {
      expect(JSON.parse(stored.value)).toBe(0);
    }
  });
});
