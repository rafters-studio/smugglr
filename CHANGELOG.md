# Changelog

All notable changes to smugglr are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.6.4] - 2026-11-11

The publishing release. The SQLite built into smugglr moves to 3.54.1, past
RUSTSEC-2026-0459, and pushing a version tag now publishes smugglr's eight
crates to crates.io, each with a README. Patch release: direct sync, the
stash blob, the memory format and the mesh wire are unchanged.

### Added

- A release publishes the eight crates to crates.io in dependency order,
  through crates.io trusted publishing (#675, PR #676): smugglr-core,
  smugglr-place, smugglr-memory, smugglr-sync, smugglr-multi, smugglr-mesh,
  smugglr-web, smugglr. It refuses a tag that does not name the version in
  `[workspace.package]`, before anything is published, and skips a crate
  whose version is already there, so a run that failed partway can be re-run.
  Every crate has a README; the smugglr crate's is this repository's.
  RELEASING.md gives the first publish of the six crates not yet on
  crates.io to the operator.

### Changed

- rusqlite 0.41 and migratr 0.3 (#673, PR #674). smugglr builds with Rust
  1.89 or later; it needed 1.88. SQLite links once per build, so migratr
  moves with rusqlite. migratr 0.3 adds foreign key and trigger operations:
  each names its table for a rebuild that `--migratr` carries, and an
  operation this smugglr does not know, at or after a rebuild, stops the
  carry with nothing changed on the place, as raw SQL does.
- The two linear-cost tests that timed a sync and a retrieve with the wall
  clock are deleted; they failed at random on busy shared runners (#677,
  PR #681).
- The test that a local file waits for another writer's lock no longer
  sleeps seven seconds. It checks the connection's busy timeout is 10
  seconds and sees through SQLite's busy handler that the write met the
  held lock (#679, PR #682).

### Security

- RUSTSEC-2026-0459: a SQLite before 3.54.0 can return uninitialized memory
  for a crafted `-wal` file. smugglr bundles SQLite: 0.6.3 bundles 3.53.2,
  and 0.6.4 bundles 3.54.1, through libsqlite3-sys 0.39 (#673, PR #674).

### Known issues

- A mesh node that restarts is taken by the others for a second machine
  under its name, and they stop with "two machines share the name" (#684).
  It is in 0.6.3 too.

## [0.6.3] - 2026-11-08

The exec release. `smugglr exec` runs a migration's statements on a place for
migratr, and only when the statements are the ones the local database ran and
the place is in the shape expected, so migratr's migrate-and-smuggle flow can
carry a schema change to D1 through smugglr. `smugglr sync <a> <b> --discover`
writes nothing and says what two places are and what a sync between them would
meet. A sync now writes a parent's rows before its children's, so a place that
enforces foreign keys, as D1 always does, takes them. Patch release: exec and
`--discover` are additive; direct sync, the stash blob, the memory format and
the mesh wire are unchanged.

### Added

- `smugglr exec <local> <place> --version <v> --up|--down --expect <fp>`
  (#657, PR #664). It reads the batch on stdin and runs it on the place in one
  all-or-nothing batch with the version's ledger row, or its removal for
  `--down`, only when the batch's SHA-256 equals the up or down hash the local
  database recorded and the place's structural fingerprint is the one
  `--expect` names; a down batch needs the local database to have run the down
  first. It prints the place's fingerprint after. `smugglr exec <db> --read`
  prints the schema, the ledger and the fingerprint, and takes no query. A
  D1 place and a file in the same shape have the same fingerprint. A refusal
  exits 3 and leaves the place as it was, a failing statement exits 1 with
  nothing changed, and a usage error exits 2. Exec is for migratr, so
  `--help` does not name it.
- `smugglr sync <a> <b> --discover` (#659, PR #665). It writes nothing and
  prints, first, every finding a sync could not get past: a table over the
  place's limit (50,000,000 bytes for D1), a foreign-key cycle, a shape
  difference. Then, for each place, its identity (for D1, the account and the
  database's uuid), its ledger, its tables with rows and bytes estimated from
  a 1,000-row sample, the copy order, self-references, triggers, views,
  indexes and ignored tables. It exits 3 when a sync would stop and 0
  otherwise; with `--json` the output carries `identity`, `ledger`,
  `blocking` and `shapes_match`. A sync that stops with exit 3 or 5 ends its
  stderr with the `--discover` command for the same two places.

### Changed

- The scenario that checks D1 and a file share one fingerprint runs locally
  on D1's own engine through miniflare, and is no longer tagged network or
  credentialed (#666, PR #667).

### Fixed

- A sync wrote tables in name order, so into a place that enforces foreign
  keys a child table that sorts before its parent failed every row on every
  run. A place now names each table's parents from `pragma_foreign_key_list`,
  and a sync writes parents first. A reference names its parent in any case,
  as SQLite's table names do, so `REFERENCES Org(id)` names the
  table `org` (#652, PR #656).
- `smugglr --version` and `-V` print the version and exit 0. They exited 2
  with the usage text, and only a bare `smugglr` printed the version; migratr
  checks `smugglr --version` before it uses exec (#662, PR #663).
- The linear-cost tests compare ten times the rows against a limit of thirty
  times as long, counting the quickest of three smaller runs, so a runner's
  noise no longer fails linear code; the larger run has a deadline, so a scan
  per row fails in about a minute instead of running to the job's time limit
  (#648, PR #649).

## [0.6.2] - 2026-11-05

The D1 schema release. `smugglr sync <local> <place> --migratr [dir]` carries
a migratr migration from a local database to a place such as D1, with no
wrangler: the first run syncs, snapshots the local database, checks the
snapshot, runs the migration locally and stops for the person to check it; the
second run carries it and syncs. Tested against real D1 with an added column,
a rebuild of a table with two tables beneath it, and a place that changed
during the check. Patch release: direct sync, the stash blob, the memory
format and the mesh wire are unchanged.

### Added

- `--migratr [dir]` and a `[migratr]` config section holding `dir` and `safe`
  (#639). With `safe` on, the default, a place that changed since the snapshot
  stops the carry and names the rows, and nothing changes on it. A migration
  that rebuilds a table drops and makes again that table and every table that
  references it on the place, then sends their rows with the local database
  winning. A place with tables and no ledger, whose shape matches, has its
  ledger recorded and runs nothing. A stopped run exits non-zero.
- A table keyed `INTEGER PRIMARY KEY` syncs (#641). The record names it once
  with a link to `docs/integer-keys.md`, which draws what two places handing
  out the same key do to each other. Under `--sync-mode last` such a table
  still stops.
- A mesh row too large for one datagram crosses in parts, and every sync names
  a value over the limit (#600).
- `smugglr mesh remove <node>`, the name check, and the batch check (#628). A
  node away past the sunset takes a stash from a peer over the mesh (#630).

### Changed

- `_migratr_migrations` is ignored by every sync, without being listed. A
  database's ledger is written only by the migrations it ran (#642).
- After a carry the place's ledger holds the local database's `applied_at`
  values, so the two ledgers match (#642).
- An edit made to the local database during the check reaches the place on
  the carry (#642).
- The mesh's default port is 31415 (#605).

### Fixed

- A place holding tables the config ignores matched no local shape, so every
  `--migratr` carry stopped. The checks now leave out what the sync leaves
  out, and a rebuild that would drop an ignored table stops first (#644).
- `install.sh` is back, for the archives the release publishes (#636).
- A whole retrieve decides a row changed here and in the blob by
  `--sync-mode`, and names it (#608).
- Mesh: a conflict kept under `--sync-mode last` lands (#613); a change resent
  to an acknowledging peer backs off (#617); changes a heard peer holds leave
  the send window (#616); the record names a peer that stopped acknowledging
  (#615); a peer named by host name is looked up at each announcement (#621);
  a change with every part acknowledged has time to be decided (#632).

### Known limits

- D1 refuses a query result over 50 MB, and smugglr reads a D1 table in one
  query, so a table whose rows come to more than that stops the sync with exit
  3 and nothing written. Ignore it and move it another way until reads come in
  pages.
- A rebuild of a table that a table in the ignore list references stops before
  anything is dropped.

## [0.6.1] - 2026-10-26

The mesh release. `smugglr mesh <place> --key-file <path>` keeps a SQLite
database in step with every machine on the local network that holds the same
key, with nobody in charge: each node finds its own changes as they happen,
sends each as a signed, sealed UDP datagram to the peers it has heard, and
applies what it hears through the same decision path as sync. 0.6.0 wrote the
mesh down as a protocol; 0.6.1 builds it, and adds `--replay` so a change
dropped past its sunset can be decided again from the command line. Patch
release: the mesh fills the `--sync-type mesh` slot 0.6.0 reserved; direct
sync, the stash blob and the memory format are unchanged, and the mesh's own
state lives in new files beside them.

Not in 0.6.1 (#521): a node back from longer than the sunset does not catch
up by itself, and instead prints the `smugglr stash` and `smugglr retrieve`
commands that do it; there is no `smugglr mesh remove`; and nothing checks
for two machines with one name, which ignore each other as themselves. Limits
of the mesh as built (#520): a previous key stays accepted for as long as it
is in the key file, since nothing retires it; a row too large for one
datagram (about 9 KB on macOS, the system's limit) is refused loudly, named
with the stash-and-retrieve catch-up, rather than sent; and the check that
the key file is readable only by its owner runs on unix only.

### Added

- **`smugglr mesh`** (PR #593, #520): `smugglr mesh <place> --key-file <path>
  [--sync-mode stop|last]`, also reachable as `smugglr sync <place>
  --sync-type mesh`, runs one node until stopped. A node is named by its
  machine's name and listens on the UDP port in the `port` setting (31337 when
  not set). It announces itself by broadcast every 10 seconds, and to the
  addresses in the `peers` setting on a network that carries no broadcast;
  its peers are the nodes it hears under the shared key, and there is no
  member list. A node finds its own changes by comparing each row's hash with
  its last hash per key, and sends each change unicast to every peer it
  knows, sealed with XChaCha20-Poly1305 under a key derived from the shared
  key apart from the one that signs. A change it hears is decided by
  smugglr-core's one decision path and written through the place, at most
  20,000 changes per table in one transaction, then acknowledged to its maker
  and echoed by broadcast once unless others already did. The maker resends
  unicast to each peer that has not acknowledged until every one has or the
  sunset passes: at once for a copy a peer skipped, within 300 ms for a peer
  that is acknowledging, and on a 10 to 160 second doubling for a silent one.
  Unacknowledged changes wait in an outbox in smugglr's own files and survive
  a restart; at the `backpressure` setting (6 MB when not set) the node takes
  no new changes until acknowledgements empty half the outbox. The record
  names every change a node holds, stops or drops, once. `--sync-mode source`
  and `target` are refused, since a mesh has no source or target.
- **`smugglr mesh keygen`** (PR #593, #520): `smugglr mesh keygen --key-file
  <path>` writes a new key at the end of the key file, creating the file when
  there is none. The file holds one base64 key per line, newest last; a node
  sends under the newest, opens datagrams under the newest key and the one
  before it (older keys in the file do not open), and answers a peer in the key
  it speaks, so appending a key rotates to it. A
  key is never written to output or errors, only its id. A key file that its
  group or others can read is refused, on unix.
- **`--replay <change hash>` on `smugglr mesh` and `smugglr retrieve`** (PR
  #595, #523): a mesh node keeps each change it drops past its sunset, the
  envelope and its values, in a dropped log in its journal, and names each
  drop by its change hash. `smugglr mesh <place> --key-file <path> --replay
  <hash>` decides that change again on this node with a fresh sunset,
  overriding its drop, prints what came of it and exits; what it writes goes
  out on the node's next run. `smugglr retrieve <place> --key-file <path>
  --node <name> --replay <hash>` does the same from multi's dropped log and
  takes no blob. An unknown hash is a usage error, exit 2.
- **The mesh settings** (PR #593, #520): config gains `port`, the mesh's UDP
  port; `backpressure`, the most the outbox holds, in bytes or as text like
  `max_wire`; and `peers`, addresses as `host` or `host:port` that a node
  announces itself to where broadcast does not reach.
- **A node back from past the sunset says so** (PR #593, #520): a node notes
  when it last ran, and started after being away longer than the sunset (the
  `graveyard` setting), it prints how long it was away, that changes made
  meanwhile may not reach it, and the `smugglr stash --whole` and `smugglr
  retrieve` commands that catch it up, then keeps running for new changes.

### Changed

- **`--sync-type multi` with `--sync-mode source` or `target`** (PR #595,
  #523): `smugglr sync` exits 2 naming `stop` and `last` as the modes multi
  takes, rather than saying only that multi is not built. `--sync-type hub`
  and `multi` remain unbuilt.

### Fixed

- **The README names the release archives as published** (PR #592, #591):
  the install section names `smugglr-<target>.tar.gz` for the four unix
  targets and `smugglr-x86_64-pc-windows-msvc.zip` for Windows, the files the
  GitHub release carries.

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
