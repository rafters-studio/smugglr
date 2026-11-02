# The mesh sweep

Real `smugglr mesh` processes behind a proxy that drops 10% of datagrams, with rows inserted, edited and deleted throughout and some nodes asleep. Two-node tests do not find what this finds: it took silent rows from 313 in 60 runs to 0 in 150 while #521 was built, and most of the bugs behind that number showed on three or more nodes.

A row is **silent** when it differs at the end and no node's record names it. A row is **back** when it was deleted, is still on a node, and is not named. Neither may be above 0. Rows that differ and are named are expected, since edit-versus-delete conflicts are stopped and named, and this churn makes them often.

    cargo build --release -p smugglr
    SUNSET=30 python3 xtask/sweep/drive.py target/release/smugglr results.jsonl 20 one,two,later,three,toomuch,inside

That is about 25 minutes. Run it when a change touches how a node decides, settles, resends or catches up, and put the table it prints in the PR. A silent row's history, from every node's journal and outbox, is in `results.jsonl.records`.

| Layout | What it does |
|---|---|
| `one`, `two`, `three` | that many nodes asleep past the sunset |
| `later` | two asleep past the sunset, the second going later |
| `toomuch` | one asleep inside the sunset, with peers owing it over `mesh_wire` |
| `inside` | one asleep inside the sunset |

`big.py <binary> <rows> <work dir>` times one large stash and prints peak memory on both nodes. 100,000 rows of a 60 MB database took 22 s and about 950 MB each side.

Not covered: echoes, because they go by broadcast and the proxy carries unicast; and anything over loopback's speed and timing.
