// Auto-sync runtime: empty-state hydration on init + sync-on-reconnect.
//
// Browser-only. Bails (returns a no-op handle) when navigator.locks or
// globalThis.addEventListener are absent so Node consumers can pass
// `autoSync: {...}` without branching.

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

const DEFAULT_BACKOFF: Required<AutoSyncBackoff> = {
  initialMs: 1000,
  maxMs: 300_000,
  jitter: true,
};
const ONLINE_DEBOUNCE_MS = 250;

export function startAutoSync(opts: {
  target: AutoSyncTarget;
  config: AutoSyncConfig;
  source: EndpointConfig;
  dest: EndpointConfig | undefined;
  sync: SyncOptions | undefined;
}): AutoSyncRuntime {
  if (!opts.dest) return noopRuntime();

  const g = globalThis as unknown as {
    addEventListener?: (ev: string, cb: () => void) => void;
    removeEventListener?: (ev: string, cb: () => void) => void;
    navigator?: {
      locks?: { request: (name: string, cb: () => Promise<void>) => Promise<void> };
    };
  };
  if (!g.addEventListener || !g.navigator?.locks) {
    return noopRuntime();
  }

  const onInit = opts.config.onInit ?? "hydrate-if-empty";
  const onReconnect = opts.config.onReconnect ?? true;
  const backoff = { ...DEFAULT_BACKOFF, ...(opts.config.backoff ?? {}) };
  const lockName = opts.config.lockName ?? defaultLockName(opts.dest);
  const locks = g.navigator.locks;

  let stopped = false;
  // Each retry loop ("pull" from init, "sync" from online) backs off
  // independently: a failing pull must not inflate the sync's next delay, and
  // a new sync must not orphan the pull's pending retry timer.
  const attempts: Record<"pull" | "sync", number> = { pull: 0, sync: 0 };
  // Every live retry timer, so stop() can clear them all -- not just the
  // most-recently-assigned one.
  const retryTimers = new Set<ReturnType<typeof setTimeout>>();
  let onlineDebounce: ReturnType<typeof setTimeout> | null = null;
  const onlineHandler = () => {
    if (onlineDebounce !== null) clearTimeout(onlineDebounce);
    onlineDebounce = setTimeout(() => {
      onlineDebounce = null;
      void runWithRetry("sync");
    }, ONLINE_DEBOUNCE_MS);
  };

  async function runOnce(kind: "pull" | "sync"): Promise<void> {
    await locks.request(lockName, async () => {
      if (stopped) return;
      if (kind === "pull") await opts.target.pull();
      else await opts.target.sync();
      attempts[kind] = 0;
    });
  }

  async function runWithRetry(kind: "pull" | "sync"): Promise<void> {
    if (stopped) return;
    try {
      await runOnce(kind);
    } catch {
      if (stopped) return;
      const delay = nextDelay(backoff, attempts[kind]++);
      const timer: ReturnType<typeof setTimeout> = setTimeout(() => {
        retryTimers.delete(timer);
        void runWithRetry(kind);
      }, delay);
      retryTimers.add(timer);
    }
  }

  const ready = (async () => {
    if (onInit === "never") return;
    if (onInit === "always") {
      await runWithRetry("pull");
      return;
    }
    const empty = await isLocalEmpty(opts.source, opts.sync?.tables);
    if (empty) await runWithRetry("pull");
  })();

  if (onReconnect) g.addEventListener("online", onlineHandler);

  return {
    ready,
    stop() {
      stopped = true;
      for (const timer of retryTimers) clearTimeout(timer);
      retryTimers.clear();
      if (onlineDebounce !== null) clearTimeout(onlineDebounce);
      if (onReconnect) g.removeEventListener?.("online", onlineHandler);
    },
  };
}

function noopRuntime(): AutoSyncRuntime {
  return { stop() {}, ready: Promise.resolve() };
}

function defaultLockName(dest: EndpointConfig): string {
  if (isLocal(dest)) return "smugglr:auto:local";
  return `smugglr:auto:${dest.url}`;
}

function isLocal(endpoint: EndpointConfig): endpoint is Extract<EndpointConfig, { type: "local" }> {
  return "type" in endpoint && endpoint.type === "local";
}

function nextDelay(backoff: Required<AutoSyncBackoff>, attempt: number): number {
  const base = Math.min(backoff.initialMs * 2 ** attempt, backoff.maxMs);
  return backoff.jitter ? Math.random() * base : base;
}

async function isLocalEmpty(
  source: EndpointConfig,
  tables: string[] | undefined,
): Promise<boolean> {
  if (!isLocal(source)) return false;
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

/**
 * Quote a SQL identifier, refusing anything that is not a plain
 * `[A-Za-z_][A-Za-z0-9_]*` name. Shared by autoSync's empty-table probe and
 * the persist-binding core (`persistBinding.ts`) -- both build table-name
 * SQL from caller-supplied strings and need the same guard against
 * injection via a crafted `table` option.
 */
export function quoteIdent(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(`autoSync: refusing unsafe table identifier "${name}"`);
  }
  return `"${name}"`;
}
