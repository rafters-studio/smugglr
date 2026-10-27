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
