# Changelog

All notable changes to smugglr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.0] - 2026-10-25

The greenfield release. smugglr 0.6.0 is a new engine, rebuilt from service
design and specification rather than from the 0.5 code (#529), so this entry
describes what 0.6.0 does, not how it differs from 0.5. It keeps two SQLite
databases in step: `smugglr sync <source> <target>` decides each row from its
hash and what the pair last agreed, writes only where a row is still as it was
read, reads every write back before calling the run done, and survives being
cut off at any point. The same engine runs in a browser tab as `@smugglr/web`,
and a person's own devices can be kept in step through blobs with `smugglr
stash` and `smugglr retrieve`. Hub, multi and mesh sync types are not in this
release: `--sync-type hub`, `multi` and `mesh` exit 2 saying they are not built
yet, and the mesh exists only as a written protocol. Minor release: a new
architecture and a new command line; nothing of 0.5's commands, plugins or
packages carries over.

### Added

- **One row hash, native and in WebAssembly** (PR #532, #499; PR #547, #534;
  PR #572, #524): every row is hashed as BLAKE3 over each column's SQLite
  `quote()` text, each prefixed with its byte length, kept to 16 bytes. The
  hash is computed where the data lives, so a place returns keys and hashes
  and sends row values only for the keys the engine asks for. A known-answer
  test pins the algorithm, and a Playwright scenario shows the browser's
  SQLite hashing 2^53+1, the i64 bounds, NULL, empty text and an empty blob
  exactly as native does.
- **`smugglr sync <source> <target>`** (PR #536, #501; PR #541, #515; PR #542,
  #509; PR #533, #500): smugglr keeps each key's last agreed hash per table in
  its config directory, one folder per pair of places, and turns each place's
  difference from that memory into a change decided through one decision path:
  a change already applied is skipped, one whose starting hash matches the row
  is applied, and the rest wait. A write applies only where the row is still
  at the hash it was read with, so a row edited mid-run is decided again on
  the next run rather than overwritten. A local file and an HTTP-SQL service
  take each write batch in one transaction; D1's REST API offers none, so a
  D1 batch that fails part-way can leave earlier rows written, and the journal
  and read-back cover it. Rows are read, journaled, written and read back
  20,000 at a time, each step its own transaction, so another connection
  waits for one chunk rather than the whole sync, and the cost of a sync or a
  retrieve grows linearly with the rows it writes (PR #581, #577; PR #584,
  #580). Flags:
  `--dryrun`, `--json`, `--sync-type direct` (the only type built),
  `--sync-mode`, `--table <name>` (repeatable) and `--watch [seconds]` (60 by
  default; `--watch 0` is refused, PR #559, #558).
- **`--sync-mode stop|source|target|last`** (PR #537, #509; PR #552, #510): a
  row changed in both places stops its table and is named (`stop`, the
  default), or the source's or target's copy is kept and recorded as an
  overwrite. `last` keeps the copy whose steering column is further on, by
  SQLite's own ordering; the column is set in config once or per table, and a
  table with none, or without the named column, is refused before any row is
  read. A stop stops only its table.
- **Places: a local file, any HTTP-SQL service, and Cloudflare D1** (PR #535,
  #503; PR #544, #504; PR #557, #555): a place is any SQLite database smugglr
  can reach behind the `Place` interface, so anyone can write an adapter. A
  bare path or `file://` opens a local file; `http://` and `https://` open an
  HTTP-SQL service over rustls, with integers beyond 2^53 and blobs carried
  without loss; `d1://<name>` reaches D1 through an adapter over its REST API,
  with the account and token from `CLOUDFLARE_ACCOUNT_ID` and
  `CLOUDFLARE_API_TOKEN`, and a malformed D1 answer stops the run rather than
  reading as empty. Another engine, such as `postgres://`, is refused with
  "smugglr keeps SQLite in step with SQLite."
- **A run is done only when its writes read back** (PR #538, #511): after
  writing a table, smugglr reads back each row it wrote and hashes it; a row
  that does not match stops the table, which keeps its old memory.
- **The journal: an interrupted run does no harm** (PR #540, #502; PR #584,
  #564): while a run writes, smugglr flushes one journal line per row before
  writing it, and the next run finishes a cut-off run first. Memory is saved
  durably across a power cut. A measurement kills the binary with SIGKILL at
  random points over 2,000 runs. A local file waits up to 10 seconds for
  another connection's write lock.
- **Runs stop before reading when they cannot be safe** (PR #549, #506; PR
  #548, #507): any difference in column names, types or order between the two
  places stops the run before a row is read, and smugglr never writes a shape.
  A table whose database hands out its own keys (an INTEGER rowid key) stops
  with a why, since two places would give one key to two rows; a random unique
  key such as UUIDv7 syncs.
- **The first sync says what it cannot see** (PR #551, #508; PR #570, #563): a
  pair with no memory records `first_sync` true and `deletes_seen` false, and
  copies a row on one side only as new. It weighs itself first, and over
  `max_wire` (100 MB by default) it writes nothing, names `smugglr stash` then
  `smugglr retrieve`, and exits 5. A first sync records the agreed baseline
  for every table, including one that stops.
- **Ignored tables** (PR #550, #505): `sqlite_*` tables are always ignored,
  and config lists more with `*` and `?` wildcards. An ignored table is never
  read, written, hashed or sent, and the record names it.
- **The record and exit codes** (PR #539, #512; PR #553, #513; PR #554, #514):
  every run returns a record, as text or JSON, of what moved per table and
  why anything stopped; inside a program `smugglr::sync` returns the same
  record, and a stop is a record, not an error. The CLI exits 0 when done or
  previewed, 2 on a usage error, 3 when it stopped before writing, 4 when it
  wrote something and is not done, and 5 when the run is too much for the
  wire.
- **`smugglr stash` and `smugglr retrieve`** (PR #574, #519; PR #568, #517;
  PR #571, #569; PR #573, #518): stash writes one blob of signed envelopes,
  the rows changed since the device's last stash (or every row with
  `--whole`), and retrieve applies blobs in any order. Each envelope is signed
  with BLAKE3 keyed by a shared key from `--key-file` and decided through the
  same decision path as sync; a transaction applies all or none. Every change
  has a sunset, the graveyard window (12 hours by default, `graveyard` in
  config); past it the change is dropped, and a delete's tombstone is kept
  that long. The bucket, the device list and crossing off are the app's;
  `examples/hono-multi` shows one way on Cloudflare R2.
- **`@smugglr/web`, the engine in a browser tab** (PR #575, #525; PR #578,
  #567): `open()` opens the tab's own SQLite database on OPFS in a worker,
  runs the app's schema, and syncs it with an endpoint the app names over the
  same direct-sync engine the CLI runs. It offers `sync()`, `stash()`,
  `retrieve()` and `replay()`, runs one sync at a time per database across
  the origin, and announces changed tables on a BroadcastChannel.
  `@smugglr/web/engine` loads the engine inside a Cloudflare Worker, and
  `examples/hono-tab` is a Hono Worker that answers a tab's place operations
  over D1 for the signed-in person's rows only. The JavaScript packages are
  not published to npm in this release; they live in the repository and
  install from `pnpm pack`.
- **`@smugglr/nanostores` and `@smugglr/zustand`** (PR #576, #565): each makes
  a store a view on one table of the tab's database. A change in the store is
  written as its row; a refused write puts the key back and reports to
  `onError`; a sync reloads only the keys it changed and never overwrites a
  change the app made meanwhile. In a server render neither touches the tab.
- **The mesh protocol, written** (PR #528, #527): `docs/mesh-protocol.md`
  specifies the wire protocol smugglr mesh will carry. The mesh itself is not
  built.

### Removed

- **The 0.5 implementation** (#529): every 0.5 command not listed above
  (`push`, `pull`, `diff`, `status`, `snapshot`, `snapshots`, `restore`,
  `broadcast`, `migrate` and the `watch` command), the plugin SDK, the
  `smugglr-http-sql` plugin and the `smugglr` npm package are gone. `stash`
  and `retrieve` keep their names but are rebuilt on the signed envelope. The
  prior code stays in git history through commit `1ff0b75`.

### Security

- **TLS advisories** (PR #561, #560): rustls-webpki 0.103.16 and webpki-roots
  1.0.10 take the fixes for RUSTSEC-2026-0382 and RUSTSEC-2026-0381 on the
  path that reaches https places and D1.
