# Generated from the legion requirement. Do not hand-edit this file.
Feature: --sync-mode last and the steering column (FR-SMUGGLR-068)

  @criterion-01a14bed-72a8-7b31-abbd-3cb7fe3b8076 @module-smugglr-core
  Scenario: last keeps the copy further on
    Given a config naming updated_at as the steering column, and a row of orders changed in both places with a later updated_at in the target
    When the agent runs smugglr sync on them with --sync-mode last
    Then the source's row holds the target's values

  @criterion-01a14bed-72a8-7b31-abbd-3ccd849d5c1e @module-smugglr-core
  Scenario: equal steering values stop
    Given a config naming updated_at as the steering column, and a row of orders changed in both places with the same updated_at
    When the agent runs smugglr sync on them with --sync-mode last --json
    Then the record gives orders the status stopped with that row's key in its rows

  @criterion-01a14bed-72a8-7b31-abbd-3cd3faaf007e @module-smugglr-core
  Scenario: a per-table steering column
    Given a config naming modified as the steering column for orders only, and a row of orders changed in both places with a later modified in the target
    When the agent runs smugglr sync on them with --sync-mode last
    Then the source's row holds the target's values

  @criterion-01a14bed-72a8-7b31-abbd-3d0d1302b90c @module-smugglr-core
  Scenario: the error comes before any write
    Given two places with rows to move, whose orders table has no steering column named in the config
    When the agent runs smugglr sync on them with --sync-mode last
    Then neither place has changed
