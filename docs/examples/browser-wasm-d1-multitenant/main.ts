// Page UI: proxies button clicks into the worker via postMessage. The worker
// owns wa-sqlite + smugglr because OPFS sync access handles are worker-only
// in WebKit and Firefox.
//
// Tenant identity comes from the ?tenant= query param. Real apps would derive
// the tenant token from an auth flow (Smugglr.updateAuth on login).

const params = new URLSearchParams(location.search);
const tenant = params.get("tenant") ?? "alice";
const tenantToken = import.meta.env[`VITE_TENANT_TOKEN_${tenant.toUpperCase()}`];
const fenceUrl = import.meta.env.VITE_FENCE_URL;

if (!tenantToken) {
  throw new Error(
    `No VITE_TENANT_TOKEN_${tenant.toUpperCase()} in .env -- add a token for tenant "${tenant}".`,
  );
}

document.getElementById("tenant-label")!.textContent = `[${tenant}]`;
document.title = `smugglr [${tenant}] -> D1`;

const worker = new Worker(new URL("./worker.ts", import.meta.url), {
  type: "module",
});

const log = document.getElementById("log") as HTMLPreElement;
const append = (line: string) => {
  log.textContent = `${new Date().toISOString().slice(11, 23)}  ${line}\n${log.textContent}`;
};

let nextId = 1;
const pending = new Map<number, (v: unknown) => void>();
worker.addEventListener("message", (ev: MessageEvent<{ id: number; ok: boolean; result?: unknown; error?: string }>) => {
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

await call("init", [tenant, tenantToken, fenceUrl]);
append(`ready as tenant=${tenant}`);

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

document.getElementById("list")!.addEventListener("click", async () => {
  const rows = await call("listRows");
  append(`local rows: ${JSON.stringify(rows)}`);
});

document.getElementById("reset")!.addEventListener("click", async () => {
  await call("reset");
  append("reset");
});
