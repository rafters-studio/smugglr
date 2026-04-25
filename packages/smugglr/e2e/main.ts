// Main-thread proxy: forwards e2e RPC calls to the worker hosting wa-sqlite + smugglr.

declare global {
  interface Window {
    e2e: {
      init(dbPath: string): Promise<unknown>;
      runSql(sql: string, params?: unknown[]): Promise<unknown>;
      sync(opts: unknown): Promise<unknown>;
      reset(): Promise<unknown>;
    };
  }
}

const worker = new Worker(new URL("./worker.ts", import.meta.url), {
  type: "module",
});

let nextId = 1;
const pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>();

worker.addEventListener("message", (ev: MessageEvent<{ id: number; ok: boolean; result?: unknown; error?: string }>) => {
  const slot = pending.get(ev.data.id);
  if (!slot) return;
  pending.delete(ev.data.id);
  if (ev.data.ok) slot.resolve(ev.data.result);
  else slot.reject(new Error(ev.data.error ?? "worker error"));
});

function call(op: string, args: unknown[]) {
  const id = nextId++;
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject });
    worker.postMessage({ id, op, args });
  });
}

window.e2e = {
  init: (dbPath) => call("init", [dbPath]),
  runSql: (sql, params = []) => call("runSql", [sql, params]),
  sync: (opts) => call("sync", [opts]),
  reset: () => call("reset", []),
};

document.getElementById("status")!.textContent = "ready";
