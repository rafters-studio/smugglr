# smugglr

## Acceptance scenarios

Acceptance criteria are Gherkin scenarios, one per criterion, tagged
`@criterion-<id>`.

- **Generated, never hand-edited.** Every `.feature` file is generated from
  the legion requirement and starts with a comment saying so. To change a
  scenario, change the requirement and regenerate.
- **Where they live.** In the crate's `tests/`, mirroring `src/`: the scenario
  for behaviour whose entry point is `src/a/b.rs` lives at `tests/a/b.feature`,
  beside any `tests/a/b.rs`.
- **Runner.** rstest-bdd (`rstest-bdd`, `rstest-bdd-macros`, and their
  required peer `rstest`, all workspace dev-dependencies), so scenarios run as
  ordinary tests under `cargo test` with no separate runner or CI workflow.
- **One runner per crate.** Each crate's `tests/scenarios.rs` calls
  `scenarios!("tests")`, which finds every `.feature` file under `tests/` and
  emits one test per scenario. Step definitions are registered per test binary,
  so a crate's steps must compile into its `tests/scenarios.rs` (directly or
  through a `mod` it declares). A runner with no `.feature` files yet runs zero
  tests; a new crate adds its own runner.
- **Unbound steps fail.** A scenario step with no matching step code fails
  `cargo test`, and the failure names the feature file and the scenario.
- **New files are picked up.** On its own, Cargo does not notice a newly added
  `.feature` file. Each crate with a runner has a `build.rs` that prints
  `cargo::rerun-if-changed=tests`, so adding or removing a file rebuilds the
  runner. A new crate's runner needs the same `build.rs`.

## Merging

The merge queue does not wait for the test jobs: its required checks are only the fast ones, so a PR can merge with its tests still running, and main goes red after. Before `legion pr merge`, run `gh pr checks <n>` and enqueue only when `test (ubuntu-latest)`, `test (macos-latest)` and `test (windows-latest)` have passed on the PR's head.

The scenario `the mesh port is a setting` runs a node on the real default port, so it fails on a machine where another program holds UDP 31415. Puck and rzr run the live legion mesh on that port, so it fails on both while the mesh runs. That is not your change; CI runs it clean.

## Traces

A defect's issue traces to a requirement with the criteria it serves, `- FR-SMUGGLR-078 [criteria: <id>, <id>]`, so `legion verify` judges only those; a bare trace asks for every criterion of the requirement. The ids are under `verification.criteria` in `legion document view <id> --json`, not at the top of the document. With none that fit, use `- None -- <why>` and verify against the issue's own criteria.

## The mesh sweep

A change to how a mesh node decides, settles, resends or catches up runs `xtask/sweep` before its PR, and the PR carries the table. Silent rows and rows back must be 0. See xtask/sweep/README.md. Two-node tests do not find what it finds.
