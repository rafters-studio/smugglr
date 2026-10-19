# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr decides what changed from each key's last hash per pair (FR-SMUGGLR-072)

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
