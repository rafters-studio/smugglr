# smugglr

smugglr keeps SQLite databases in step. One Rust binary, three kinds of place: a local file, a service that speaks HTTP-SQL, and Cloudflare D1. Each row is compared by a 16-byte BLAKE3 hash of its contents, so only rows that changed cross the wire. A row changed in both places stops and is named; nothing is overwritten unless you say so.

smugglr is not a database. It moves rows between databases and keeps its own memory in files in its config directory.

## Install

The GitHub release for tag v0.6.0 (https://github.com/rafters-studio/smugglr/releases) carries one archive per target: `smugglr-<target>.tar.gz` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin` and `aarch64-apple-darwin`, and `smugglr-x86_64-pc-windows-msvc.zip` for Windows. smugglr publishes no checksum file.

From source:

```
cargo install --git https://github.com/rafters-studio/smugglr --tag v0.6.0 smugglr
```

## First sync

The first place named is the source, the second the target. `--dryrun` writes nothing.

```
$ smugglr sync laptop/shop.db server/shop.db --dryrun
smugglr sync: preview
  first sync: a row on one side only is copied as new; deletes are not seen
  weight: 3 rows, 68 B in 1 table
  products: would move, 3 to target, 0 to source, 0 deleted in target, 0 deleted in source, 0 overwritten in target, 0 overwritten in source
```

```
$ smugglr sync laptop/shop.db server/shop.db
smugglr sync: done
  first sync: a row on one side only is copied as new; deletes are not seen
  weight: 3 rows, 68 B in 1 table
  products: moved, 3 to target, 0 to source, 0 deleted in target, 0 deleted in source, 0 overwritten in target, 0 overwritten in source
$ echo $?
0
```

Same counts. Run it again and the record says `in step`. Add `--json` and the record is one JSON object:

```
$ smugglr sync laptop/shop.db server/shop.db --json
{
  "status": "done",
  "first_sync": false,
  "deletes_seen": true,
  "tables": {
    "products": {
      "status": "moved",
      "to_target": 1,
      "to_source": 1,
      "deleted_in_target": [
        "sku-0233"
      ],
      "deleted_in_source": [],
      "overwritten_in_target": [],
      "overwritten_in_source": []
    }
  },
  "ignored": []
}
```

## A row changed in both places

smugglr stops rather than choose, names the row, and exits 3. A stop in one table does not hold the others back: when another table moved first, the run is `stopped in part` and exits 4.

```
$ smugglr sync laptop/shop.db server/shop.db
smugglr sync: stopped
  products: stopped
    rows: sku-0412
    These rows changed in both places since the last sync. smugglr stops rather than choose. Rerun with --sync-mode source or target to keep one place's copy, or name a steering column and use --sync-mode last.
$ echo $?
3
```

`--sync-mode source` keeps the source's copy, `target` the target's, `last` the copy whose steering column (named in the config) is further on. The preview names each row it would overwrite; the run overwrites exactly those.

```
$ smugglr sync laptop/shop.db server/shop.db --sync-mode source --dryrun
smugglr sync: preview
  products: would move, 1 to target, 0 to source, 0 deleted in target, 0 deleted in source, 1 overwritten in target, 0 overwritten in source
    overwritten in target: sku-0412
```

```
$ smugglr sync laptop/shop.db server/shop.db --sync-mode source
smugglr sync: done
  products: moved, 1 to target, 0 to source, 0 deleted in target, 0 deleted in source, 1 overwritten in target, 0 overwritten in source
    overwritten in target: sku-0412
```

## Places

| address | place | credentials |
| --- | --- | --- |
| `./shop.db` | a local SQLite file | none |
| `https://...` | a service that speaks HTTP-SQL | none sent |
| `d1://<database-name>` | a Cloudflare D1 database | `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN` |

The D1 token needs D1 edit permission. A missing variable is refused by name, exit 2.

smugglr keeps no secrets and has no environment of its own: it runs inside the environment of the project that calls it. Each project's `.env` holds the two variables, or a secrets manager does. With 1Password Environments:

```
op run --environment=<id> -- smugglr sync ./shop.db d1://shop
```

Before it reads a row, smugglr checks that both places hold the same tables with the same columns. A difference stops the run and writes nothing. Tables a sync should not touch in a shared database go in the config's `ignore` list, and the check skips them. `sqlite_*` and D1's `_cf_*` tables are ignored without being listed. `--table` limits which tables move, not which are checked.

## Config

`$XDG_CONFIG_HOME/smugglr/config.toml`, else `$HOME/.config/smugglr/config.toml`:

```toml
steering = "updated_at"
ignore = ["audit", "cache_*"]
max_wire = "250 MB"
graveyard = 43200

[tables.orders]
steering = "modified"
```

`max_wire` caps a first sync, 100 MB when unset. A first sync heavier than the cap stops with exit 5 and names the commands that move it. Here `max_wire = "10 B"` trips on two small rows:

```
$ smugglr sync ./laptop/shop.db ./server/shop.db | sed "s#$PWD#.#g"
smugglr sync: too much
  first sync: a row on one side only is copied as new; deletes are not seen
  Too much for one sync: 2 rows, 44 B in 1 table.
  Move it with: smugglr stash ./laptop/shop.db <dir> --key-file <path> --node <name> --whole, then smugglr retrieve ./server/shop.db <dir> --key-file <path> --node <name>
```

## A large first sync

`smugglr stash` writes rows into a signed blob in a directory; `smugglr retrieve` applies the blob at the other place. Both sides share one key file of 64 hex digits (`openssl rand -hex 32 > shop.key`) and name themselves with `--node`.

```
$ smugglr stash a/shop.db blobs --key-file shop.key --node laptop --whole
blobs/1792977241457-31250147751667d1c02b67c6ec99a5a6.blob
```

```
$ smugglr retrieve b/shop.db blobs --key-file shop.key --node phone
smugglr sync: done
  products: moved, 3 to target, 0 to source, 0 deleted in target, 0 deleted in source, 0 overwritten in target, 0 overwritten in source
  targets remaining: 
$ echo $?
0
```

A sync between those two places afterwards still counts as a first sync, so it does not see deletes. The rows already match, so none move.

Every change smugglr carries between devices has a sunset, the graveyard setting (12 hours by default), and past it the change is dead. Clocks should agree to within one twelfth of the sunset.

## Exit codes

| code | meaning |
| --- | --- |
| 0 | Done, or a preview. |
| 2 | Usage error: a bad flag or place, a missing D1 variable, a config smugglr cannot read. |
| 3 | Stopped before writing anything. |
| 4 | Wrote something and is not done. Run it again. |
| 5 | Too much for the wire. Nothing was sent. |

## Not in 0.6.0

`--sync-type` accepts only `direct`. `hub`, `multi` and `mesh` are refused as not built yet.

```
$ smugglr sync laptop/shop.db server/shop.db --sync-type mesh 2>&1 | head -1
--sync-type mesh is not built yet
```

Every output block above was pasted from a run of the release binary against local SQLite files.
