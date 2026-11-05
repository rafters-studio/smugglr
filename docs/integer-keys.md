# Keys the database hands out

smugglr syncs a table keyed `INTEGER PRIMARY KEY`, and names it once in the record:

```
  hands out its own keys: sessions (https://github.com/rafters-studio/smugglr/blob/main/docs/integer-keys.md)
```

It syncs because one place writing new rows is common and works: the place that hands out a
key is the only one that does. It is named because two places writing new rows makes a mess, and
smugglr cannot fix the mess for you. This page shows why. A random, unique key, such as UUIDv7,
lets two writers sync without any of it.

## Two places, one next number

Each SQLite database hands out the next key itself, from what it holds. Two places that were in
step hold the same rows, so they hand out the same next key, each for a different row.

```
        laptop                                 server
  orders                                 orders
  +----+-------------+                   +----+-------------+
  | 41 | alice, tent |   <-- in step --> | 41 | alice, tent |
  +----+-------------+                   +----+-------------+

  INSERT bob's order                     INSERT carol's order
  +----+-------------+                   +----+-------------+
  | 41 | alice, tent |                   | 41 | alice, tent |
  | 42 | bob, stove  |                   | 42 | carol, lamp |
  +----+-------------+                   +----+-------------+
         \                                      /
          `---------- the same key, 42 --------'
                     two different orders
```

Key 42 is now bob's order in one place and carol's in the other. To smugglr that is one row
changed in both places, because a key is how it tells rows apart.

## What each sync mode does with it

```
  --sync-mode stop     (the default)
      orders: stopped, rows: 42
      "Both places handed out these keys to different rows ..."
      laptop  42 = bob, stove        server  42 = carol, lamp      nothing written

  --sync-mode source   (laptop is the source)
      server's 42 is overwritten       laptop  42 = bob, stove
                                       server  42 = bob, stove     carol's order is gone

  --sync-mode target
      laptop's 42 is overwritten       laptop  42 = carol, lamp    bob's order is gone
                                       server  42 = carol, lamp

  --sync-mode last
      the table stops before any row is read, with an empty rows list:
      nobody is there to choose between bob and carol
```

On the orders table the default loses nothing, and stops. The way on is to choose, and every choice loses one of the
two orders, because they share a key that only one of them can have. smugglr does no
re-sequencing: it cannot give carol's order key 43, because it cannot know what else points at 42.

## What points at the key

Rows in other tables point at an order by its key. They go wrong with it, and they do not wait for
the orders table: an item has its own key, so it is a new row and moves, even while the orders
table is stopped.

```
        laptop                                 server
  order_items                            order_items
  +----------+----------+-------+        +----------+----------+-------+
  | i-laptop | order 42 | stove |        | i-server | order 42 | lamp  |
  +----------+----------+-------+        +----------+----------+-------+

  smugglr sync laptop server            (the default)
      orders:      stopped, rows: 42
      order_items: moved, 1 to target, 1 to source

  laptop  orders 42 = bob, stove      items: stove -> 42, lamp -> 42   carol's lamp is on bob's order
  server  orders 42 = carol, lamp     items: stove -> 42, lamp -> 42   bob's stove is on carol's order

  then --sync-mode source
  server  orders 42 = bob, stove      items: stove -> 42, lamp -> 42   carol's order is gone,
                                                                       her lamp stays on bob's
```

Nothing in the record says an item points at the wrong order: the item table moved its rows
correctly, row by row. The damage is in what the keys mean, and it starts with the default.

## A key reused after a delete

A table keyed `INTEGER PRIMARY KEY` without `AUTOINCREMENT` reuses a key once its row is deleted.

```
        laptop                                 server
  +----+-------------+                   +----+-------------+
  | 42 | bob, stove  |   <-- in step --> | 42 | bob, stove  |
  +----+-------------+                   +----+-------------+

  DELETE 42, then INSERT dave's order
  +----+-------------+
  | 42 | dave, rope  |    the database handed out 42 again
  +----+-------------+

  smugglr sync laptop server
      orders: moved, 1 to target
      server  42 = dave, rope
```

To smugglr, key 42 was edited, so it moves the edit, and the record reads like any other sync. Every row on the server that pointed at bob's
order now points at dave's. `AUTOINCREMENT` stops the reuse, and a random key stops all of it.

## In short

- One place writing new rows: fine.
- Two places writing new rows: every new row can collide. The default stops on each, the rows
  that point at it move anyway and point at the wrong row, and every way past a stop loses a row.
- Keys reused after a delete: the row's meaning changes and the sync reports an ordinary edit.
- `--sync-mode last`: the table stops, because nobody would choose.

A random, unique key, such as UUIDv7, avoids all of these.
