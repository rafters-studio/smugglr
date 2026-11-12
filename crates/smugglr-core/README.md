# smugglr-core

The engine of smugglr, which keeps SQLite databases in step. It hashes each row's contents (`row_hash`), plans what a sync of one table does from both sides' hashes and the last hashes it remembers (`plan`), and holds the types the other crates share: a change, the envelope a change travels in, a table's shape, the record of a run and the weight of a first sync. It does no I/O and reads no clock, and it builds for wasm32.

Depend on it to write a place adapter or to use the engine's types; it reads and writes no database on its own. To run a sync, depend on `smugglr` or `smugglr-sync`.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
