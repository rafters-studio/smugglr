// smugglr - Content-hashed delta sync for SQLite, in the browser.
//
// Typed wrapper over the smugglr-wasm bindings. Handles WASM initialization
// internally so consumers never touch __wbg_init or raw JsValues.

import type {
  SmugglrConfig,
  SyncResult,
  DiffResult,
  SyncOptions,
  EndpointConfig,
  HttpEndpointConfig,
  LocalEndpointConfig,
  SqlExecutor,
  InitOptions,
  TableChangedEvent,
  SmugglrEventMap,
  Unsubscribe,
  AutoSyncConfig,
  AutoSyncBackoff,
} from "./types.js";
import { SmugglrError } from "./types.js";
import { startAutoSync, type AutoSyncRuntime } from "./autoSync.js";

export type {
  SmugglrConfig,
  SyncResult,
  DiffResult,
  SyncOptions,
  EndpointConfig,
  HttpEndpointConfig,
  LocalEndpointConfig,
  SqlExecutor,
  InitOptions,
  TableChangedEvent,
  SmugglrEventMap,
  Unsubscribe,
  AutoSyncConfig,
  AutoSyncBackoff,
};
export { SmugglrError };
export { createWaSqliteExecutor } from "./opfs.js";
export type { WaSqlite3 } from "./opfs.js";

// WASM module state -- loaded lazily or set explicitly via setWasm().
let wasmModule: WasmModule | null = null;
let wasmReady: Promise<WasmModule> | null = null;

// Minimal interface for the wasm-bindgen output we depend on.
interface WasmModule {
  default: (input?: RequestInfo | URL | BufferSource | WebAssembly.Module) => Promise<unknown>;
  Smugglr: {
    init(config: unknown): WasmSmugglr;
    new (): never;
  };
}

interface WasmSmugglr {
  free(): void;
  push(dry_run?: boolean | null): Promise<unknown>;
  pull(dry_run?: boolean | null): Promise<unknown>;
  sync(dry_run?: boolean | null): Promise<unknown>;
  diff(): Promise<unknown>;
  on(event: string, callback: (e: unknown) => void): () => void;
  eraseLocal(): Promise<unknown>;
  updateAuth(authToken: string): void;
  updateDest(dest: unknown): void;
  [Symbol.dispose]?: () => void;
}

/**
 * Pre-load the WASM module before calling Smugglr.init().
 *
 * Useful when you want to control how/where the .wasm binary is loaded from:
 * - Custom CDN URL
 * - Pre-fetched ArrayBuffer
 * - Bundler-resolved import
 *
 * @example
 * ```ts
 * import { setWasm } from "smugglr";
 * import * as wasm from "smugglr/wasm";
 * await setWasm(wasm);
 * ```
 *
 * @example
 * ```ts
 * // Point to a CDN-hosted binary
 * import { setWasm } from "smugglr";
 * const mod = await import("smugglr/wasm");
 * await setWasm(mod, "https://cdn.example.com/smugglr_wasm_bg.wasm");
 * ```
 *
 * @param input Optional argument forwarded to the wasm-bindgen `default()`
 *   initializer (a URL/path to the `.wasm` binary, a fetched `Response`, a
 *   `BufferSource`, or a compiled `WebAssembly.Module`). Omit it to use the
 *   module's default-resolved binary.
 */
export async function setWasm(
  mod: WasmModule,
  input?: RequestInfo | URL | BufferSource | WebAssembly.Module,
): Promise<WasmModule> {
  // Actually instantiate the WebAssembly instance. Without this the
  // wasm-bindgen glue is loaded but no memory/exports exist, so the first
  // Smugglr.init() faults against an uninitialized module.
  await mod.default(input);
  wasmModule = mod;
  wasmReady = Promise.resolve(mod);
  return mod;
}

async function loadWasm(options?: InitOptions): Promise<WasmModule> {
  if (wasmReady) return wasmReady;

  wasmReady = (async () => {
    // If the consumer passed a module directly, use it.
    if (options?.wasmModule) {
      const mod = options.wasmModule as WasmModule;
      await mod.default();
      wasmModule = mod;
      return mod;
    }

    // Default: dynamic import of the co-located wasm-bindgen output.
    // This path is rewritten by the build script to point at the bundled copy.
    const mod = await import("./wasm/smugglr_wasm.js") as WasmModule;
    await mod.default(options?.wasmUrl);
    wasmModule = mod;
    return mod;
  })().catch((e) => {
    // A transient load failure (network blip, CDN 503) must not brick init
    // forever. Clear the cached rejected promise so a later init() can retry.
    wasmReady = null;
    wasmModule = null;
    throw e;
  });

  return wasmReady;
}

/** smugglr sync client for browser and Node.js */
export class Smugglr {
  private inner: WasmSmugglr;
  private auto: AutoSyncRuntime | null = null;
  private disposed = false;
  // Unsubscribe wrappers handed out by on(). dispose() neutralizes them so a
  // stale handle invoked after free() cannot deref freed WASM memory.
  private unsubs = new Set<() => void>();

  private constructor(inner: WasmSmugglr) {
    this.inner = inner;
  }

  /**
   * Initialize a smugglr sync client.
   *
   * @example
   * ```ts
   * const s = await Smugglr.init({
   *   source: { url: "https://my-db.turso.io", authToken: "tok", profile: "turso" },
   *   dest: { url: "https://api.cloudflare.com/...", authToken: "cf-tok", profile: "d1" },
   *   sync: { tables: ["users", "posts"], conflictResolution: "local_wins" }
   * });
   * ```
   *
   * @example
   * ```ts
   * // With custom WASM URL (e.g. from a CDN)
   * const s = await Smugglr.init(config, {
   *   wasmUrl: "https://cdn.example.com/smugglr_wasm_bg.wasm"
   * });
   * ```
   */
  static async init(config: SmugglrConfig, options?: InitOptions): Promise<Smugglr> {
    const wasm = await loadWasm(options);
    let inner: WasmSmugglr;
    try {
      inner = wasm.Smugglr.init(config);
    } catch (e) {
      throw new SmugglrError(String(e), 2);
    }
    const instance = new Smugglr(inner);
    if (config.autoSync) {
      instance.auto = startAutoSync({
        target: instance,
        config: config.autoSync,
        source: config.source,
        dest: config.dest,
        sync: config.sync,
      });
      await instance.auto.ready;
    }
    return instance;
  }

  /**
   * Push source rows to destination.
   * Only sends rows that actually changed (content-hashed delta).
   */
  async push(options?: { dryRun?: boolean }): Promise<SyncResult> {
    try {
      return (await this.inner.push(options?.dryRun)) as SyncResult;
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Pull destination rows to source.
   * Only fetches rows that differ from what you already have.
   */
  async pull(options?: { dryRun?: boolean }): Promise<SyncResult> {
    try {
      return (await this.inner.pull(options?.dryRun)) as SyncResult;
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Bidirectional sync with conflict resolution.
   * Pushes source changes to dest, pulls dest changes to source.
   */
  async sync(options?: { dryRun?: boolean }): Promise<SyncResult> {
    try {
      return (await this.inner.sync(options?.dryRun)) as SyncResult;
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Read-only comparison between source and destination.
   * Shows what push, pull, or sync would do without moving data.
   */
  async diff(): Promise<DiffResult> {
    try {
      return (await this.inner.diff()) as DiffResult;
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Subscribe to events emitted by this Smugglr instance.
   *
   * The `table-changed` event fires once per affected table after a `pull`
   * or `sync` completes the local write. The handler receives a
   * {@link TableChangedEvent}. Returns an unsubscribe function.
   *
   * @example
   * ```ts
   * const unsub = s.on("table-changed", (e) => {
   *   console.log(`${e.table} changed`, e.changedPks, "via", e.source);
   * });
   * await s.sync();
   * unsub();
   * ```
   */
  on<K extends keyof SmugglrEventMap>(
    event: K,
    handler: (e: SmugglrEventMap[K]) => void,
  ): Unsubscribe {
    if (this.disposed) {
      throw new SmugglrError("Smugglr.on() called after dispose()", 1);
    }
    const wrapped = (raw: unknown) => handler(raw as SmugglrEventMap[K]);
    const innerUnsub = this.inner.on(event, wrapped);
    // The inner unsubscribe dereferences a raw pointer back into the Rust
    // Smugglr (crates/smugglr-wasm/src/lib.rs on()). Once dispose() has called
    // free(), that pointer is dangling. Guard so a post-dispose unsubscribe is
    // a safe no-op instead of a use-after-free.
    const guarded: Unsubscribe = () => {
      if (this.disposed) return;
      this.unsubs.delete(guarded);
      innerUnsub();
    };
    this.unsubs.add(guarded);
    return guarded;
  }

  /**
   * Erase local state. Issues `DELETE FROM <table>` against the local
   * SQLite database for every configured sync table, then clears the
   * in-memory metadata caches. Schema and any non-synced tables stay put.
   *
   * The dest endpoint is not touched -- server-side erasure is the app's
   * concern. Use this for GDPR right-to-erasure on the client side.
   *
   * Returns the list of tables that were erased.
   */
  async eraseLocal(): Promise<{ erasedTables: string[] }> {
    try {
      return (await this.inner.eraseLocal()) as { erasedTables: string[] };
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Replace the dest auth token without re-initializing.
   * The dest URL, profile, and metadata cache are unchanged; the next
   * request just uses the new token. Use this for token rotation.
   *
   * Errors if the dest is not an HTTP endpoint.
   *
   * **Do not call while a sync future is pending.** Await any in-flight
   * push/pull/sync first.
   */
  updateAuth(authToken: string): void {
    try {
      this.inner.updateAuth(authToken);
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Replace the entire dest endpoint. Accepts the same shape as the
   * `dest` field of `Smugglr.init({...})`. Clears the dest metadata
   * cache so the next sync re-scans against the new endpoint; the
   * source cache (and local OPFS data) survive.
   *
   * Use this for the anonymous-to-account upgrade flow: start with an
   * anonymous ingress dest, swap to the account-bound dest after sign-in.
   *
   * **Do not call while a sync future is pending.** Await first.
   */
  updateDest(dest: EndpointConfig): void {
    try {
      this.inner.updateDest(dest);
    } catch (e) {
      throw parseError(e);
    }
  }

  /**
   * Cancel the auto-sync loop started by `Smugglr.init({ autoSync: ... })`.
   * Removes the `online` listener and aborts any pending retry timer. Idempotent;
   * safe to call when no auto-sync is active.
   */
  stopAutoSync(): void {
    this.auto?.stop();
    this.auto = null;
  }

  /** Release WASM resources. Called automatically if using `using` syntax. */
  dispose(): void {
    if (this.disposed) return;
    this.stopAutoSync();
    // Drain listeners on the JS side and neutralize outstanding unsubscribe
    // handles BEFORE free(), so any handle a consumer still holds becomes an
    // inert no-op rather than a deref of freed WASM memory.
    for (const unsub of this.unsubs) unsub();
    this.unsubs.clear();
    this.disposed = true;
    this.inner.free();
  }

  [Symbol.dispose](): void {
    this.dispose();
  }
}

function parseError(e: unknown): SmugglrError {
  const message = String(e);
  // Map error messages to exit codes based on smugglr-core error semantics
  if (message.includes("Config") || message.includes("config")) {
    return new SmugglrError(message, 2);
  }
  if (
    message.includes("timeout") ||
    message.includes("503") ||
    message.includes("429") ||
    message.includes("rate limit")
  ) {
    return new SmugglrError(message, 3);
  }
  if (message.includes("conflict") || message.includes("concurrent")) {
    return new SmugglrError(message, 4);
  }
  if (message.includes("not found") || message.includes("404")) {
    return new SmugglrError(message, 5);
  }
  if (message.includes("plugin") || message.includes("Plugin")) {
    return new SmugglrError(message, 6);
  }
  return new SmugglrError(message, 1);
}
