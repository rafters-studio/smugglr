// wa-sqlite ships its VFS implementations as untyped JS under src/examples.
// Declare the one this example registers so the import is typed. SQLiteVFS is
// the global interface wa-sqlite's own types declare for vfs_register().

declare module "wa-sqlite/src/examples/OriginPrivateFileSystemVFS.js" {
  export const OriginPrivateFileSystemVFS: new () => SQLiteVFS;
}
