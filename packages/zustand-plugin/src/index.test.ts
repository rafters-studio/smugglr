import { afterEach, describe, expect, it, vi } from "vitest";
import { createStore } from "zustand/vanilla";
import type { SqlExecutor, TableChangedEvent } from "smugglr";

import { smuggl } from "./index.js";

interface Row {
  key: string;
  value: string;
  updated_at: string;
}

/** In-memory fixture executor that mimics the persistence table. */
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

/** Minimal Smugglr stub: just the .on() surface the middleware uses. */
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

const flushMicrotasks = () => new Promise((r) => setTimeout(r, 0));

afterEach(() => {
  vi.restoreAllMocks();
});

describe("@smugglr/zustand", () => {
  it("hydrates from existing row on store creation", async () => {
    const { executor } = fixtureExecutor([
      { key: "todos", value: JSON.stringify({ items: ["alpha", "beta"] }), updated_at: "x" },
    ]);
    const smugglr = fixtureSmugglr();

    interface State {
      items: string[];
      add: (s: string) => void;
    }
    const useStore = createStore<State>()(
      smuggl(
        (set) => ({
          items: [],
          add: (s) => set((prev) => ({ items: [...prev.items, s] })),
        }),
        { smugglr, executor, table: "app_state", key: "todos" },
      ),
    );

    await flushMicrotasks();
    expect(useStore.getState().items).toEqual(["alpha", "beta"]);
  });

  it("persists on every set()", async () => {
    const { executor, rows } = fixtureExecutor();
    const smugglr = fixtureSmugglr();

    interface State {
      n: number;
      inc: () => void;
    }
    const useStore = createStore<State>()(
      smuggl(
        (set) => ({
          n: 0,
          inc: () => set((prev) => ({ n: prev.n + 1 })),
        }),
        {
          smugglr,
          executor,
          table: "app_state",
          key: "counter",
          include: (s) => ({ n: s.n }),
        },
      ),
    );

    await flushMicrotasks();
    useStore.getState().inc();
    useStore.getState().inc();
    await flushMicrotasks();

    const stored = rows.get("counter");
    expect(stored).toBeDefined();
    expect(JSON.parse(stored!.value)).toEqual({ n: 2 });
  });

  it("re-hydrates when smugglr reports table-changed for the same key", async () => {
    const { executor, rows } = fixtureExecutor([
      { key: "todos", value: JSON.stringify({ items: [] }), updated_at: "t0" },
    ]);
    const smugglr = fixtureSmugglr();

    interface State {
      items: string[];
    }
    const useStore = createStore<State>()(
      smuggl(
        () => ({ items: [] }),
        { smugglr, executor, table: "app_state", key: "todos" },
      ),
    );

    await flushMicrotasks();
    expect(useStore.getState().items).toEqual([]);

    // Simulate a sync that pulled new content for this row.
    rows.set("todos", {
      key: "todos",
      value: JSON.stringify({ items: ["from-remote"] }),
      updated_at: "t1",
    });
    smugglr.fire({
      table: "app_state",
      changedPks: ["todos"],
      removedPks: [],
      source: "pull",
    });

    await flushMicrotasks();
    expect(useStore.getState().items).toEqual(["from-remote"]);
  });

  it("ignores table-changed events for other tables or keys", async () => {
    const { executor } = fixtureExecutor([
      { key: "todos", value: JSON.stringify({ items: [] }), updated_at: "t0" },
    ]);
    const smugglr = fixtureSmugglr();
    const onHydrate = vi.fn();

    createStore(
      smuggl(
        () => ({ items: [] }),
        { smugglr, executor, table: "app_state", key: "todos", onHydrate },
      ),
    );

    await flushMicrotasks();
    expect(onHydrate).toHaveBeenCalledTimes(1);

    // Wrong table.
    smugglr.fire({ table: "other", changedPks: ["todos"], removedPks: [], source: "pull" });
    // Right table, wrong key.
    smugglr.fire({ table: "app_state", changedPks: ["other"], removedPks: [], source: "pull" });

    await flushMicrotasks();
    expect(onHydrate).toHaveBeenCalledTimes(1);
  });
});
