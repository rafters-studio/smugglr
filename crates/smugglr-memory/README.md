# smugglr-memory

smugglr's memory: for each pair of places, each key's last hash per table from the last finished run, kept in smugglr's own files and never in a place. `Store` is the interface; `Files` keeps it in smugglr's config directory, one folder per pair of places, with the journal of a run that is writing.

Depend on it when you run smugglr-sync, smugglr-multi or smugglr-mesh with a memory of your own; the smugglr CLI already keeps it for you.

smugglr's README: https://github.com/rafters-studio/smugglr#readme
