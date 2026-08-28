# cli-migrate

`smugglr migrate` moves schema the way the rest of the tool moves rows: an inspectable manifest instead of a raw SQL string, a checksum, and a ledger inside the database that refuses to apply the same thing twice. This example adds one table to the Westwind sample. No credentials, no network, no config file: `migrate` runs before any config is loaded, so a migration applies where you point it and nowhere else.

## Prerequisites

`smugglr` 0.5.0 on your PATH, the `sqlite3` shell, and the [westwind](../westwind/) sample in this repository.

## Setup

Build the sample here and make a directory for manifests. The database must exist before `migrate apply`: it opens read-write without create, on the reasoning that migrating a database that does not exist is a mistake rather than a request to make one.

```sh
../westwind/make.sh ./westwind.db
mkdir -p migrations
```

## Run

`migrate new` scaffolds a manifest from a Rails-style column spec and prints it to stdout. Keys default to `TEXT`, the shape smugglr's primary-key requirement wants; add `notnull` so the first-run check's second layer, which wants a declared key to be non-nullable, has nothing to say. In 0.5.0 the name must be `create_<table>`; the alter form is deferred.

```
$ smugglr migrate new create_bribes id:pk:notnull harbormaster:text amount:int paid_at:int > migrations/create_bribes.json
```

The manifest is an `up` list and a `down` list of typed ops, each carrying an `op_class`, plus a checksum over the whole thing. This one is a `create_table` going up and a `drop_table` coming down:

```json
{
  "manifest": {
    "version": 1,
    "target_schema": "",
    "up": [
      {
        "op": {
          "op": "create_table",
          "table": "bribes",
          "columns": [
            { "name": "id", "kind": "text", "constraints": [ { "constraint": "pk" }, { "constraint": "not_null" } ] },
            { "name": "harbormaster", "kind": "text" },
            { "name": "amount", "kind": "int" },
            { "name": "paid_at", "kind": "int" }
          ],
          "without_rowid": false
        },
        "op_class": "additive"
      }
    ],
    "down": [
      { "op": { "op": "drop_table", "table": "bribes" }, "op_class": "destructive" }
    ],
    "flags": { "destructive": false, "hash_rewriting": false }
  },
  "checksum": "62f1d23e0c3b34222075d7f99ed090c4971710642a5201af5ad51bdb99d90148"
}
```

`migrate apply` takes the manifest and an explicit `--db`. The ledger assigns the version, `current + 1`, and records the checksum. Westwind was itself built by eight applies, so this one lands as v9:

```
$ smugglr migrate apply migrations/create_bribes.json --db ./westwind.db
Applied migration v9 (1 op) -- checksum 62f1d23e0c3b34222075d7f99ed090c4971710642a5201af5ad51bdb99d90148
```

Apply it again and the ledger answers before any op runs:

```
$ smugglr migrate apply migrations/create_bribes.json --db ./westwind.db
Migration v9 is already applied -- nothing to do
```

The table is there, and the ledger now holds nine rows: Westwind's own eight migrations, which `make.sh` applied through the same command, and this one:

```
$ sqlite3 westwind.db ".schema bribes"
CREATE TABLE IF NOT EXISTS "bribes" ("id" TEXT PRIMARY KEY NOT NULL, "harbormaster" TEXT, "amount" INTEGER, "paid_at" INTEGER);

$ sqlite3 westwind.db "select version, status, substr(checksum,1,16) from _smugglr_migrations order by version"
1|success|f40fed16bdb497a9
2|success|846afb23867541c0
3|success|d45e3062fb7d3ebf
4|success|c6447ae278a2e42d
5|success|c0bef91533708c8d
6|success|78ef6eae8eac9227
7|success|4b5cce42abd1e797
8|success|282301d633321a57
9|success|62f1d23e0c3b3422
```

## What the ledger does

`_smugglr_migrations` lives inside the migrated database: version, checksum, status, a lease for crash recovery, and a hash chain over prior rows. The version is assigned by the ledger as `current + 1`, claimed before the first op runs and settled to `success` or `failed` after, so a manifest's own `version` field says nothing about where it lands. Each op is idempotent, and an apply interrupted midway converges on re-run instead of double-applying. The chain is written on every apply; nothing verifies it on a production path yet (#327).

## What 0.5.0 does not do

Reverse exists in the library and is not wired to a `migrate` subcommand. `--paranoid` warns instead of taking a recovery snapshot until #289 lands. The D1, Turso, and rqlite dialects generate statements but refuse to execute them (#291). There is no `int -> UUIDv7` conversion for an existing table (#280), and `migrate new` does not yet refuse an explicit `int:pk` (#427). Every one of those is stated here so that nobody discovers it in production.

## What this demonstrates

A schema change as data: the manifest can be read, diffed, checked into git, and checksummed before it touches anything. A ledger that makes re-running safe by refusing, not by hoping. And the same primary-key discipline as sync: the scaffold defaults every key to a text column, because an integer key is the one thing this tool refuses to move.

Every block on this page was captured from smugglr 0.5.0 on 2026-08-28 by running the commands shown, in this directory, in this order, against the committed Westwind seed.
