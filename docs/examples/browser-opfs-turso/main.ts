// Page UI: proxies button clicks into the worker via postMessage. The worker
// owns wa-sqlite + smugglr because OPFS sync access handles are worker-only
// in WebKit and Firefox.

const tursoUrl = import.meta.env.VITE_TURSO_URL;
const tursoToken = import.meta.env.VITE_TURSO_TOKEN;
if (typeof tursoUrl !== "string" || typeof tursoToken !== "string") {
  throw new Error("VITE_TURSO_URL and VITE_TURSO_TOKEN must be set in .env");
}

const worker = new Worker(new URL("./worker.ts", import.meta.url), {
  type: "module",
});

const log = document.getElementById("log") as HTMLPreElement;
const append = (line: string) => {
  log.textContent = `${new Date().toISOString().slice(11, 23)}  ${line}\n${log.textContent}`;
};

type WorkerMessage =
  | { id: number; ok: boolean; result?: unknown; error?: string }
  | { event: string; detail: unknown };

let nextId = 1;
const pending = new Map<number, (v: unknown) => void>();
worker.addEventListener("message", (ev: MessageEvent<WorkerMessage>) => {
  if ("event" in ev.data) {
    append(`${ev.data.event}: ${JSON.stringify(ev.data.detail)}`);
    return;
  }
  const slot = pending.get(ev.data.id);
  if (!slot) return;
  pending.delete(ev.data.id);
  slot(ev.data.ok ? ev.data.result : { error: ev.data.error });
});

function call(op: string, args: unknown[] = []) {
  const id = nextId++;
  return new Promise<unknown>((resolve) => {
    pending.set(id, resolve);
    worker.postMessage({ id, op, args });
  });
}

await call("init", [tursoUrl, tursoToken]);
append("ready");

document.getElementById("add")!.addEventListener("click", async () => {
  const id = crypto.randomUUID();
  const ts = new Date().toISOString();
  await call("addRow", [id, ts]);
  append(`add: ${id}`);
});

document.getElementById("sync")!.addEventListener("click", async () => {
  const result = await call("sync");
  append(`sync: ${JSON.stringify(result)}`);
});

document.getElementById("reset")!.addEventListener("click", async () => {
  await call("reset");
  append("reset");
});
