# smugglr-sync

One direct sync between two places: read each place's hashes, plan each table, weigh a first sync, write, and keep the pair's memory. `sync` runs it with `Options` and returns the record of the run. The smugglr command line and a browser tab both run this same sync.

Depend on it to run a sync between two `smugglr-place` places from your own program without the CLI's config. The `smugglr` crate re-exports `sync` and `Options` along with reading the config.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
