// smugglr npm package type definitions
// These provide typed interfaces over the raw wasm-bindgen output.

/** HTTP SQL endpoint -- syncs against any HTTP SQL backend (D1, Turso, rqlite, etc.). */
export interface HttpEndpointConfig {
  /** Full URL to the HTTP SQL endpoint */
  url: string;
  /** Authentication token (Bearer for most profiles, Basic for rqlite) */
  authToken?: string;
  /** Profile name: d1, turso, rqlite, datasette, sqlitecloud, starbasedb, generic */
  profile?: string;
}

/**
 * Local SQLite executor contract.
 *
 * Any SQLite runtime can plug in -- wa-sqlite + OPFS (browser), better-sqlite3
 * (Node), official sqlite-wasm, sql.js, or a future runtime. Smugglr is
 * SQLite-runtime-agnostic; consumers provide an executor that satisfies this
 * shape and the diff/sync engine speaks SQL strings against it.
 *
 * Result shape:
 * - `columns`: column names in declaration order
 * - `rows`: each row is an array of values aligned with `columns`
 *
 * Implementations should bind `params` positionally (`?` placeholders).
 */
export interface SqlExecutor {
  run(sql: string, params: unknown[]): Promise<{
    columns: string[];
    rows: unknown[][];
  }>;
}

/** Local SQLite endpoint -- syncs against an in-process SQLite database. */
export interface LocalEndpointConfig {
  type: "local";
  /** SQL executor wrapping a SQLite runtime (wa-sqlite/OPFS, better-sqlite3, etc.). */
  executor: SqlExecutor;
}

/** Either side of a sync may be a remote HTTP endpoint or a local SQLite database. */
export type EndpointConfig = HttpEndpointConfig | LocalEndpointConfig;

/** Sync behavior configuration */
export interface SyncOptions {
  /** Tables to sync (empty = all non-excluded tables) */
  tables?: string[];
  /** Tables to exclude from sync */
  excludeTables?: string[];
  /** Column patterns to exclude from content hashing (glob: "*_embedding", "vector") */
  excludeColumns?: string[];
  /** Column name for timestamp-based conflict resolution (default: "updated_at") */
  timestampColumn?: string;
  /** Conflict resolution strategy */
  conflictResolution?: "local_wins" | "remote_wins" | "newer_wins" | "uuid_v7_wins";
  /** Maximum rows per write batch (default: 100) */
  batchSize?: number;
}

/** Top-level configuration for Smugglr.init() */
export interface SmugglrConfig {
  /** Source endpoint (data is read from here for push, written to here for pull) */
  source: EndpointConfig;
  /** Destination endpoint (data is written to here for push, read from here for pull) */
  dest: EndpointConfig;
  /** Sync behavior options */
  sync?: SyncOptions;
}

/** Per-table sync result */
export interface TableResult {
  name: string;
  rowsPushed?: number;
  rowsPulled?: number;
}

/** Result from push, pull, or sync operations */
export interface SyncResult {
  command: "push" | "pull" | "sync";
  status: "ok" | "dry_run";
  tables: TableResult[];
}

/** Per-table diff breakdown */
export interface TableDiff {
  name: string;
  localOnly: number;
  remoteOnly: number;
  localNewer: number;
  remoteNewer: number;
  contentDiffers: number;
  identical: number;
}

/** Result from diff operation */
export interface DiffResult {
  command: "diff";
  status: "ok";
  tables: TableDiff[];
}

/** Options for WASM initialization */
export interface InitOptions {
  /** URL to the .wasm binary (overrides the default co-located path) */
  wasmUrl?: string | URL;
  /** Pre-imported WASM module (from `import * as wasm from "smugglr/wasm"`) */
  wasmModule?: unknown;
}

/** Exit code semantics matching the CLI */
export type ExitCode = 0 | 1 | 2 | 3 | 4 | 5 | 6;

/** Typed error with exit code semantics */
export class SmugglrError extends Error {
  readonly code: ExitCode;
  readonly retryable: boolean;

  constructor(message: string, code: ExitCode = 1) {
    super(message);
    this.name = "SmugglrError";
    this.code = code;
    this.retryable = code === 3;
  }
}
