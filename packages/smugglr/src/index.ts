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
} from "./types.js";
import { SmugglrError } from "./types.js";

export type {
  SmugglrConfig,
  SyncResult,
  DiffResult,
  SyncOptions,
  EndpointConfig,
};
export { SmugglrError };

// The WASM module is loaded lazily on first Smugglr.init() call.
let wasmReady: Promise<typeof import("../../crates/smugglr-wasm/pkg/smugglr_wasm.js")> | null =
  null;

async function loadWasm() {
  if (!wasmReady) {
    wasmReady = import("../../crates/smugglr-wasm/pkg/smugglr_wasm.js").then(
      async (mod) => {
        await mod.default();
        return mod;
      }
    );
  }
  return wasmReady;
}

/** smugglr sync client for browser and Node.js */
export class Smugglr {
  private inner: InstanceType<
    Awaited<ReturnType<typeof loadWasm>>["Smugglr"]
  >;

  private constructor(inner: unknown) {
    this.inner = inner as typeof this.inner;
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
   */
  static async init(config: SmugglrConfig): Promise<Smugglr> {
    const wasm = await loadWasm();
    try {
      const inner = wasm.Smugglr.init(config);
      return new Smugglr(inner);
    } catch (e) {
      throw new SmugglrError(String(e), 2);
    }
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

  /** Release WASM resources. Called automatically if using `using` syntax. */
  dispose(): void {
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
