# smugglr examples

End-to-end examples covering the main ways people reach for smugglr. Each example has a self-contained `README.md` with prerequisites, setup, and the command(s) to run it.

## The sample database

| Example | What it shows |
| ------- | ------------- |
| [westwind](./westwind) | The sample every example below uses: eight tables, forty customers, three hundred and twenty orders, built by eight `smugglr migrate` manifests. `./make.sh` builds it; `./make.sh --empty` builds a target with the tables and no rows. |

## CLI

| Example | What it shows |
| ------- | ------------- |
| [cli-sqlite-to-sqlite](./cli-sqlite-to-sqlite) | Two local SQLite files, `push --dry-run`, `push`, `diff`. No credentials. The shortest path to real output. |
| [cli-stash-file-relay](./cli-stash-file-relay) | `stash` from one database and `retrieve` into another through a `file://` relay. The S3 workflow with no S3. |
| [cli-migrate](./cli-migrate) | `migrate new` scaffolds a manifest, `migrate apply` applies it and refuses to apply it twice. |
| [cli-d1-sync](./cli-d1-sync) | `config.toml` + `smugglr push/pull/sync` against Cloudflare D1. Blocked in 0.5.0 by #429 and #430; the README says how. |
| [cli-lan-broadcast](./cli-lan-broadcast) | Two machines on the same subnet converging over encrypted UDP multicast. |

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
| [browser-wasm-d1-multitenant](./browser-wasm-d1-multitenant) | Many browsers, each its own SQLite, syncing into one shared D1 partitioned by `tenant_id`. Includes a Cloudflare Worker that authenticates, scopes reads, and validates writes. |

## Notes on running these

Every output block in these READMEs was pasted from a run of the 0.5.0 binary or package, and the command that produced it sits above it. An example that needs credentials (D1, Turso) says so in its prerequisites and shows no output it could not capture; an example with no network dependency (the three `cli-*` examples above the D1 one, the Rust examples, the LAN broadcast) runs as written. These directories are the source of truth for config shapes and CLI output: the README at the repository root and smugglr.dev copy their blocks from here, not the other way around.
