# Generated from the legion requirement. Do not hand-edit this file.
Feature: what changed, from each key's last hash per pair, and --sync-mode (FR-SMUGGLR-072, FR-SMUGGLR-067)

  @criterion-01a14bed-813e-7e03-9baf-886e8e7289a6 @module-smugglr-core
  Scenario: an unknown key is new
    Given two places synced before, and a row since added to the source
    When the agent runs smugglr sync on them
    Then the target holds that row

  @criterion-01a14bed-813e-7e03-9baf-8878d4c740c7 @module-smugglr-core
  Scenario: a known key gone on one side was deleted there
    Given two places synced before, and a row since deleted in the source
    When the agent runs smugglr sync on them
    Then the row is gone from the target

  @criterion-01a14bed-813e-7e03-9baf-888807681a4e @module-smugglr-core
  Scenario: a one-sided edit is carried
    Given two places synced before, and a row since edited in the target only
    When the agent runs smugglr sync on them
    Then the source's row holds the target's values

  @criterion-01a14bed-6ee4-73d2-a7eb-4ede6c89fd8d @module-smugglr-core
  Scenario: a both-sides change stops and is named
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --json
    Then the record gives notes the status stopped with that row's key in its rows

  @criterion-01a14bed-6ee4-73d2-a7eb-4ee6dafa5a8a @module-smugglr-core
  Scenario: the stop says why
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --json
    Then the why for notes says the rows changed in both places and names --sync-mode

  @criterion-01a14bed-6ee4-73d2-a7eb-4ef2a287d783 @module-smugglr-core
  Scenario: edit against delete stops
    Given two places synced before, and a row of notes since edited in the source and deleted in the target
    When the agent runs smugglr sync on them with --json
    Then the record gives notes the status stopped with that row's key in its rows

  @criterion-01a14bed-6ee4-73d2-a7eb-4f0ac39253ff @module-smugglr-core
  Scenario: a stop stops only its table
    Given two places where notes has a row changed in both and orders has rows to move
    When the agent runs smugglr sync on them with --json
    Then the record gives orders the status moved

  @criterion-01a14bed-6ee4-73d2-a7eb-4f185635fec9 @module-smugglr-core
  Scenario: source keeps the source's copy
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --sync-mode source
    Then the target's row holds the source's values

  @criterion-01a14bed-6ee4-73d2-a7eb-4f2e5961b908 @module-smugglr-core
  Scenario: source names what it overwrote
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --sync-mode source --json
    Then the record's overwritten_in_target for notes holds that row's key

  @criterion-01a14bed-6ee4-73d2-a7eb-4f3ebbf94efc @module-smugglr-core
  Scenario: target keeps the target's copy
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --sync-mode target
    Then the source's row holds the target's values

  @criterion-01a14bed-6ee4-73d2-a7eb-4f481043b8fe @module-smugglr-core
  Scenario: target names what it overwrote
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --sync-mode target --json
    Then the record's overwritten_in_source for notes holds that row's key

  @criterion-01a14bed-6ee4-73d2-a7eb-4f6eb5a59be0 @module-smugglr-core
  Scenario: no word for a better copy in the record
    Given two places synced before, and a row of notes since edited in both
    When the agent runs smugglr sync on them with --sync-mode source --json
    Then the record does not contain the word wins
