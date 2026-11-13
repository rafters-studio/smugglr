# The measurements

Real `smugglr` processes against the built binary, in real time. These are measurements, not tests: they take minutes, so they are not part of `cargo test`. Each prints its seed, so a run can be repeated, and then its result. It exits 0 when the measurement meets its NFR and 1 when it does not.

    cargo build -p smugglr
    cargo run -p measure -- <subcommand> <smugglr binary> <work dir>

The smugglr binary is `target/debug/smugglr`, or `target\debug\smugglr.exe` on Windows.

| Subcommand | What it measures | How long | NFR |
|---|---|---|---|
| `cut-off` | 2,000 `smugglr sync` runs between two local files, each killed at a random point, with rows inserted, edited and deleted on both sides before and after the cut, each followed by a rerun. Both files are then compared with what the people meant: deletes undone, rows lost, edits lost, reruns not done must all be 0. | minutes | NFR-SMUGGLR-003 |
| `multi-whole` | Three devices kept in step by `smugglr stash` and `smugglr retrieve` through a local directory standing in for the bucket, with the sunset at 10 s. A device is offline inside the sunset, then a device is offline past five sunsets, then blobs are retrieved in any order. Each device is compared with what the people meant: rows lost, deletes undone, edits lost must all be 0. | about a minute | NFR-SMUGGLR-006 |
| `mesh-whole` | Five `smugglr mesh` nodes on one machine behind a relay that drops 10% of datagrams, with the sunset at 600 s and one node stopped for 400 s and restarted. After quiet, each node is compared with what the people meant. The awake nodes must have 0 lost; the sleeper's result is printed as found. | about twelve minutes | NFR-SMUGGLR-005 |

The work directory holds each run's databases and config. `cut-off` removes each run's folder after it finishes; the others leave theirs for reading.
