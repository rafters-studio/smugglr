# @smugglr/web

A browser tab's own SQLite database, kept in its origin private file system
and kept in step by the smugglr engine in the tab with an endpoint the app
names.

## Limits

Measured on the built package in headless Chromium, against a stand-in
endpoint answering with gzip-compressed responses, `maxWire` at 200 MB, rows
of 200 bytes. The run is `crates/smugglr-web/tests/load.rs`:

```
cargo test -p smugglr-web --test load -- --ignored --nocapture --test-threads 1
```

| Limit | Measured |
| --- | --- |
| A tab killed mid-sync | 500 syncs killed at random points (234 tabs closed, 266 renderer processes killed; 255 still syncing when killed), each reopened and synced: 0 rows lost, 0 written twice, 0 deletes undone. |
| First sync of 100 MB | Not yet published. |
| Longest main-thread block while 100 MB lands | Not yet published. |
| Rows at which a first sync is refused for weight | Not yet published. |
| Rows at which a tab gives out | Not yet published. |
| A cleared tab rebuilding 100 MB, by first sync and by retrieve | Not yet published. |

The load limits wait for a fix to the engine: a sync's time grows with the
square of the rows it writes, so numbers measured now would not be the
package's limits.
