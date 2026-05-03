// Auto-sync runtime: empty-state hydration on init + sync-on-reconnect.
//
// Browser-only. Bails (returns a no-op handle) when navigator.locks,
// navigator.storage, or globalThis.addEventListener are absent so Node
// consumers can pass `autoSync: {...}` without branching.

import type {
  AutoSyncBackoff,
  AutoSyncConfig,
  EndpointConfig,
  SqlExecutor,
  SyncOptions,
} from "./types.js";

/** Subset of the Smugglr instance auto-sync drives. */
export interface AutoSyncTarget {
  pull(): Promise<unknown>;
  sync(): Promise<unknown>;
}

export interface AutoSyncRuntime {
  stop(): void;
  /** Resolves once the init-phase trigger (if any) has finished. */
  ready: Promise<void>;
}

interface SidecarState {
  lastSyncAt: number | null;
}

const DEFAULT_SIDECAR = ".smugglr/auto-sync.json";
const DEFAULT_BACKOFF: Required<AutoSyncBackoff> = {
  initialMs: 1000,
  maxMs: 300_000,
  jitter: true,
};

export function startAutoSync(opts: {
  target: AutoSyncTarget;
  config: AutoSyncConfig;
  source: EndpointConfig;
  dest: EndpointConfig | undefined;
  sync: SyncOptions | undefined;
}): AutoSyncRuntime {
  // No dest: nothing to sync against. Spec says no-op.
  if (!opts.dest) return noopRuntime();

  // Need a browser-ish env (online events, locks, storage). Otherwise no-op.
  const g = globalThis as unknown as {
    addEventListener?: (ev: string, cb: () => void) => void;
    removeEventListener?: (ev: string, cb: () => void) => void;
    navigator?: {
      locks?: { request: (name: string, cb: () => Promise<void>) => Promise<void> };
      storage?: { getDirectory: () => Promise<FileSystemDirectoryHandle> };
    };
  };
  if (!g.addEventListener || !g.navigator?.locks || !g.navigator?.storage) {
    return noopRuntime();
  }

  const onInit = opts.config.onInit ?? "hydrate-if-empty";
  const onReconnect = opts.config.onReconnect ?? true;
  const backoff = { ...DEFAULT_BACKOFF, ...(opts.config.backoff ?? {}) };
  const sidecarPath = opts.config.sidecarPath ?? DEFAULT_SIDECAR;
  const lockName = opts.config.lockName ?? defaultLockName(opts.dest);

  const sidecar = createSidecar(sidecarPath, g.navigator.storage);
  const locks = g.navigator.locks;

  let stopped = false;
  let attempt = 0;
  let retryTimer: ReturnType<typeof setTimeout> | null = null;
  const onlineHandler = () => { void runWithRetry("sync"); };

  async function runOnce(kind: "pull" | "sync"): Promise<void> {
    await locks.request(lockName, async () => {
      if (stopped) return;
      if (kind === "pull") await opts.target.pull();
      else await opts.target.sync();
      await sidecar.write({ lastSyncAt: Date.now() });
      attempt = 0;
    });
  }

  async function runWithRetry(kind: "pull" | "sync"): Promise<void> {
    if (stopped) return;
    try {
      await runOnce(kind);
    } catch {
      if (stopped) return;
      const delay = nextDelay(backoff, attempt++);
      retryTimer = setTimeout(() => { void runWithRetry(kind); }, delay);
    }
  }

  const ready = (async () => {
    if (onInit === "never") return;
    if (onInit === "always") {
      await runWithRetry("pull");
      return;
    }
    // hydrate-if-empty: only pull when every configured table has zero rows.
    const empty = await isLocalEmpty(opts.source, opts.sync?.tables);
    if (empty) await runWithRetry("pull");
  })();

  if (onReconnect) g.addEventListener("online", onlineHandler);

  return {
    ready,
    stop() {
      stopped = true;
      if (retryTimer !== null) clearTimeout(retryTimer);
      if (onReconnect) g.removeEventListener?.("online", onlineHandler);
    },
  };
}

function noopRuntime(): AutoSyncRuntime {
  return { stop() {}, ready: Promise.resolve() };
}

function defaultLockName(dest: EndpointConfig): string {
  if ("type" in dest && dest.type === "local") return "smugglr:auto:local";
  return `smugglr:auto:${(dest as { url: string }).url}`;
}

function nextDelay(backoff: Required<AutoSyncBackoff>, attempt: number): number {
  const base = Math.min(backoff.initialMs * 2 ** attempt, backoff.maxMs);
  return backoff.jitter ? Math.random() * base : base;
}

async function isLocalEmpty(
  source: EndpointConfig,
  tables: string[] | undefined,
): Promise<boolean> {
  // Only meaningful for local sources. HTTP source -> caller is doing
  // server-to-server sync, which has no "empty local" semantic.
  if (!("type" in source) || source.type !== "local") return false;
  if (!tables || tables.length === 0) return false;

  const exec: SqlExecutor = source.executor;
  for (const table of tables) {
    const safe = quoteIdent(table);
    const result = await exec.run(
      `SELECT EXISTS(SELECT 1 FROM ${safe} LIMIT 1) AS has_row`,
      [],
    );
    const cell = result.rows[0]?.[0];
    if (Number(cell) === 1) return false;
  }
  return true;
}

function quoteIdent(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`autoSync: refusing unsafe table identifier "${name}"`);
  }
  return `"${name}"`;
}

interface Sidecar {
  write(state: SidecarState): Promise<void>;
}

function createSidecar(
  path: string,
  storage: { getDirectory: () => Promise<FileSystemDirectoryHandle> },
): Sidecar {
  const segments = path.split("/").filter(Boolean);
  const fileName = segments.pop();
  if (!fileName) throw new Error(`autoSync: invalid sidecarPath "${path}"`);

  return {
    async write(state) {
      let dir = await storage.getDirectory();
      for (const seg of segments) {
        dir = await dir.getDirectoryHandle(seg, { create: true });
      }
      const handle = await dir.getFileHandle(fileName, { create: true });
      const writable = await handle.createWritable();
      await writable.write(JSON.stringify(state));
      await writable.close();
    },
  };
}
