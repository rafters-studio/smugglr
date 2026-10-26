# smugglr

smugglr keeps SQLite databases in step. One Rust binary, three kinds of place: a local file, a service that speaks HTTP-SQL, and Cloudflare D1. On a local network where nobody is in charge, `smugglr mesh` keeps the nodes in step as changes happen. Each row is compared by a 16-byte BLAKE3 hash of its contents, so only rows that changed cross the wire. A row changed in both places stops and is named; nothing is overwritten unless you say so.

smugglr is not a database. It moves rows between databases and keeps its own memory in files in its config directory.

## Install

The GitHub release for tag v0.6.1 (https://github.com/rafters-studio/smugglr/releases) carries one archive per target: `smugglr-<target>.tar.gz` for `x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`, `x86_64-apple-darwin` and `aarch64-apple-darwin`, and `smugglr-x86_64-pc-windows-msvc.zip` for Windows. smugglr publishes no checksum file.

From source:

```
cargo install --git https://github.com/rafters-studio/smugglr --tag v0.6.1 smugglr
```

## First sync

The first place named is the source, the second the target. `--dryrun` writes nothing.

```
$ smugglr sync laptop/shop.db server/shop.db --dryrun
smugglr sync: preview
  products: would move, 1 to target, 1 to source, 1 deleted in target, 0 deleted in source, 0 overwritten in target, 0 overwritten in source
    deleted in target: sku-0233
```

```
$ smugglr sync laptop/shop.db server/shop.db
smugglr sync: stopped
  products: stopped
    rows: sku-0412
    These rows changed in both places since the last sync. smugglr stops rather than choose. Rerun with --sync-mode source or target to keep one place's copy, or name a steering column and use --sync-mode last.
$ echo $?
3
```

Same counts. Run it again and the record says `in step`. Add `--json` and the record is one JSON object:

```
$ smugglr sync laptop/shop.db server/shop.db --json
{
  "status": "stopped",
  "first_sync": false,
  "deletes_seen": true,
  "tables": {
    "products": {
      "status": "stopped",
      "rows": [
        "sku-0412"
      ],
      "why": "These rows changed in both places since the last sync. smugglr stops rather than choose. Rerun with --sync-mode source or target to keep one place's copy, or name a steering column and use --sync-mode last."
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
blobs/1793029348893-b3e7393d49f72c5cb0802f80bfc414ca.blob
```

```
$ smugglr retrieve b/shop.db blobs --key-file shop.key --node phone
smugglr sync: done
  targets remaining: 
```

A retrieve drops changes the same way, and names each with its change hash. `smugglr retrieve <place> --key-file <path> --node <name> --replay <change hash>` takes no blob; it decides the dropped change again and exits. A change dropped here is a row re-created inside the graveyard window:

```
$ smugglr retrieve b/shop.db blobs/1793029352129-f96dca92cc240975f6584d8930ab08a1.blob --key-file shop.key --node phone
smugglr sync: done
  dropped: notes n1, its key is in the graveyard, change f83318454e7df3a77410b8f707dd567c327e59763e5390426f002830016c348e
  targets remaining: 
```

```
$ smugglr retrieve b/shop.db --key-file shop.key --node phone --replay f83318454e7df3a77410b8f707dd567c327e59763e5390426f002830016c348e
smugglr sync: done
  notes: moved, 1 to target, 0 to source, 0 deleted in target, 0 deleted in source, 0 overwritten in target, 0 overwritten in source
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

## Mesh

`smugglr mesh <place> --key-file <path>` keeps one database in step with every other node on the local network that holds the same key. There is no server, no hub and no coordinator, and any node can leave while the rest go on. Each change goes out as it happens and is resent to each peer until that peer has decided it. `smugglr sync <place> --sync-type mesh --key-file <path>` is the same command. The protocol is in `docs/mesh-protocol.md`.

A mesh is one shared key. `keygen` writes it with `chmod 600`; copy the file to every node.

```
$ smugglr mesh keygen --key-file ./mesh.key
smugglr mesh: wrote key 180627566f7a4e3b to ./mesh.key
```

The key file holds one base64 key per line, newest last; a line starting with `#` is a note. A node signs and seals with the newest key and also accepts the one before it, so a key is rotated by appending a new one. On unix a node refuses a key file its group or others can read, and exits 2:

```
$ chmod 644 ./loose.key
$ smugglr mesh ./puck/node.db --key-file ./loose.key
./loose.key: the key file is readable by its group or others; chmod 600 ./loose.key fixes it
$ echo $?
2
```

Every datagram is sealed with XChaCha20-Poly1305 under a key derived from the shared key, and each envelope is signed under another, so a capture of the traffic holds no row value and no key. No key is ever printed; a key is named by its `key_id`.

A node announces itself by broadcast and learns each peer's address from where its datagrams come from. After that it talks to peers by unicast. On a network that carries no broadcast (WireGuard, Tailscale, a cloud network) the `peers` setting names the addresses to announce to. A node is named by its machine's name, `hostname` or `COMPUTERNAME` on Windows, so every node needs its own name. The two nodes below share one machine, each with its own port and a one-line `hostname` script ahead of the real one on its `PATH`, and name each other in `peers`:

```toml
# puck's config.toml
port = 31401
peers = ["127.0.0.1:31402"]
```

```toml
# rzr's config.toml
port = 31402
peers = ["127.0.0.1:31401"]
graveyard = 3
```

`graveyard = 3` on rzr is for the sunset demonstration below; leave it out for a real mesh.

Both tables exist before the nodes start. A node's first run notes the rows already in its database and sends none of them; use stash and retrieve to bring a new node up to date.

```
$ smugglr mesh ./node.db --key-file ../mesh.key   # on puck
smugglr mesh: node puck on UDP port 31401, key 180627566f7a4e3b
$ smugglr mesh ./node.db --key-file ../mesh.key   # on rzr
smugglr mesh: node rzr on UDP port 31402, key 180627566f7a4e3b
```

A row written on puck reaches rzr with no sync run, whichever program wrote it. A delete crosses the same way.

```
$ sqlite3 ./node.db "insert into notes values('n1','first note');"   # on puck
$ sqlite3 ./node.db "select * from notes"   # on rzr
n1|first note
# rzr, output so far
smugglr mesh: node rzr on UDP port 31402, key 180627566f7a4e3b
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
```

```
$ sqlite3 ./node.db "delete from notes where id='n1'"   # on puck
$ sqlite3 ./node.db "select count(*) from notes"   # on rzr
0
# rzr, output so far
smugglr mesh: node rzr on UDP port 31402, key 180627566f7a4e3b
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
```

A node that starts after being away longer than the sunset (the `graveyard` setting, 12 hours by default; 3 seconds here) says so, and names the commands that catch it up. It keeps running for new changes.

```
$ smugglr mesh ./node.db --key-file ../mesh.key   # on rzr, restarted after being stopped longer than its graveyard (3 seconds); the scratch directory is shown as .
smugglr mesh: node rzr on UDP port 31402, key 180627566f7a4e3b
smugglr mesh: this node has been away 7 seconds, longer than the sunset (3 seconds); changes made meanwhile may not reach it. Catch up with: smugglr stash <a peer's place> <dir> --key-file <path> --node <name> --whole, then smugglr retrieve ./node.db <dir> --key-file <path> --node rzr
```

A change too large for one datagram is refused loudly and dropped from the outbox, so it does not hold up the changes after it. On macOS the limit is about 9 KB (`net.inet.udp.maxdgram`). Move the row with stash and retrieve.

```
$ sqlite3 ./node.db "insert into notes values('big', <30,000 characters>);"   # on puck
# puck, output so far
smugglr mesh: node puck on UDP port 31401, key 180627566f7a4e3b
smugglr mesh: 1 change arrived only as a resend; the first copy was lost
smugglr mesh: a change to notes ["'big'"] is too large for one datagram and is not sent; catch it up with smugglr stash and smugglr retrieve
```

### Dropped changes and replay

A node decides each change it hears. An insert for a key deleted inside the graveyard window is dropped: the record names it with its change hash, and the node keeps the change and its values in a dropped log. Here puck deletes `n1`, then writes it again, and rzr drops the second insert:

```
$ sqlite3 ./node.db "insert into notes values('n1','back again');"   # on puck, after the delete above
$ sqlite3 ./node.db "select count(*) from notes"   # on rzr
0
# rzr, output so far
smugglr mesh: node rzr on UDP port 31402, key 180627566f7a4e3b
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
smugglr mesh: dropped a change to notes ["'n1'"] from puck: its key is in the graveyard (change 79838289357156ba44a650973925ce44c6567e6017139d68acf10aae9ee8cd43)
```

`--replay <change hash>` decides that logged change again on this node, with a fresh sunset, overriding the drop. It prints what came of it and exits; what it wrote reaches the other nodes when the node next runs. An unknown hash is a usage error, exit 2.

```
$ smugglr mesh ./node.db --key-file ../mesh.key --replay 79838289357156ba44a650973925ce44c6567e6017139d68acf10aae9ee8cd43   # on rzr
smugglr mesh: applied 1 change to notes from puck, key 180627566f7a4e3b
smugglr mesh: replayed change 79838289357156ba44a650973925ce44c6567e6017139d68acf10aae9ee8cd43
$ echo $?
0
$ sqlite3 ./node.db "select * from notes"   # on rzr
n1|back again
```

| setting | default | what it does |
| --- | --- | --- |
| `port` | 31337 | the UDP port a node broadcasts and listens on |
| `peers` | none | addresses to announce to, `host` or `host:port` |
| `backpressure` | 6 MB | the most the outbox of unacknowledged changes holds; at the line the node takes no new changes in until acknowledgements bring it down |
| `graveyard` | 12 hours | seconds a change is carried; sets each change's sunset |

Designed and tested for up to 50 nodes. Clocks should agree to within one twelfth of the sunset.

### Not in 0.6.1

- Automatic catch-up past the sunset (#521). A node away longer than the sunset prints the commands above; the person runs `smugglr stash` and `smugglr retrieve`.
- `smugglr mesh remove` (#521). A node gone for good stops being a target when the sunset passes.
- Retiring the previous key. A node accepts the key before the newest with no end date.
- A loud name clash (#521). Two machines with one name ignore each other and nothing says so.
- The `mesh_wire` setting, which belongs to the automatic catch-up.

## Not in 0.6.1

`--sync-type` accepts `direct` and `mesh`. `hub` and `multi` are refused as not built yet.

```
$ smugglr sync laptop/shop.db server/shop.db --sync-type hub 2>&1 | head -1
--sync-type hub is not built yet
```

Every output block above was pasted from a run of the binary against local SQLite files; the mesh blocks come from two nodes on one machine.
