# smugglr examples

End-to-end examples covering the main ways people reach for smugglr. Each example has a self-contained `README.md` with prerequisites, setup, and the command(s) to run it.

## CLI

| Example | What it shows |
| ------- | ------------- |
| [cli-d1-sync](./cli-d1-sync) | `config.toml` + `smugglr push/pull/sync` against Cloudflare D1. Hello-world. |
| [cli-lan-broadcast](./cli-lan-broadcast) | Two laptops on the same subnet auto-syncing via UDP. Offline-tolerant ops demo. |

## Node (`smugglr` npm)

| Example | What it shows |
| ------- | ------------- |
| [node-server-to-d1](./node-server-to-d1) | Node script reads a local SQLite file and `.push()`es it to D1. |
| [node-auto-sync](./node-auto-sync) | `setInterval` wrapping `.sync()` with backoff on failure. |

## Rust (`smugglr-core` library)

| Example | What it shows |
| ------- | ------------- |
| [rust-tokio-service](./rust-tokio-service) | Embedded sync inside a long-running tokio service. Bypasses the CLI. |
| [rust-custom-datasource](./rust-custom-datasource) | Implementing `DataSource` against a non-standard store. Explains content-hashed delta vs CDC. |

## Browser (`smugglr` npm + wa-sqlite)

| Example | What it shows |
| ------- | ------------- |
| [browser-opfs-turso](./browser-opfs-turso) | wa-sqlite + `OriginPrivateFileSystemVFS` (OPFS), syncing to Turso. The local-first golden path. |
| [browser-idb-turso](./browser-idb-turso) | `IDBBatchAtomicVFS` (IndexedDB) variant. Compatibility for older Safari and embedded webviews. |

## Notes on running these

- **Network-dependent examples** (D1, Turso) describe the prerequisites (account, token, table) but obviously need your credentials to actually run. Code is verbatim what we use ourselves.
- **Local-only examples** (LAN broadcast, Rust embedded, custom DataSource, browser OPFS against a local mock) are runnable as-is.
- Every example uses only OSS-tier features. Nothing here depends on smugglr+/fence profiles.
