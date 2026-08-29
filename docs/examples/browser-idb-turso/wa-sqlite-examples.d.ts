// wa-sqlite ships its VFS implementations as untyped JS under src/examples.
// Declare the one this example registers so the import is typed. SQLiteVFS is
// the global interface wa-sqlite's own types declare for vfs_register().

declare module "wa-sqlite/src/examples/IDBBatchAtomicVFS.js" {
  export interface IDBBatchAtomicVFSOptions {
    durability?: "default" | "strict" | "relaxed";
    purge?: "deferred" | "manual";
    purgeAtLeast?: number;
  }

  export const IDBBatchAtomicVFS: new (
    idbDatabaseName?: string,
    options?: IDBBatchAtomicVFSOptions,
  ) => SQLiteVFS;
}
