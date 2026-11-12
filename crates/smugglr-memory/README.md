# smugglr-memory

smugglr's memory: for each pair of places, each key's last hash per table from the last finished run, kept in smugglr's own files and never in a place. `Store` is the interface; `Files` keeps it in smugglr's config directory, one folder per pair of places, with the journal of a run that is writing.

Depend on it to embed smugglr-multi or smugglr-mesh: their `Settings` take a `Store`, such as `Files`, and neither crate re-exports it. A smugglr-sync user needs it only to keep memory somewhere other than smugglr's config directory. The smugglr CLI keeps memory for you.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
