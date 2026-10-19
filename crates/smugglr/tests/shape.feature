# Generated from the legion requirement. Do not hand-edit this file.
Feature: any shape mismatch stops the run before a row is read (FR-SMUGGLR-070)

  @criterion-01a14bed-7869-73b3-8435-7af9ecc26051 @module-smugglr-core
  Scenario: a shape mismatch stops the whole run
    Given two places whose orders table has a column in the target that the source lacks
    When the agent runs smugglr sync on them with --json
    Then the record's run status is stopped

  @criterion-01a14bed-7869-73b3-8435-7b06141f1ccd @module-smugglr-core
  Scenario: a shape mismatch moves nothing
    Given two places whose orders table has a column in the target that the source lacks, and whose notes table has rows to move
    When the agent runs smugglr sync on them
    Then neither place has changed

  @criterion-01a14bed-7869-73b3-8435-7b1576b79fa4 @module-smugglr-core
  Scenario: the mismatch names the difference
    Given two places whose orders table has a column in the target that the source lacks
    When the agent runs smugglr sync on them with --json
    Then the record names that column

  @criterion-01a14bed-7869-73b3-8435-7b2a3e659629 @module-smugglr-core
  Scenario: migratr versions differ
    Given two places holding migratr's ledger at different latest migration versions
    When the agent runs smugglr sync on them with --json
    Then the record names both versions

  @criterion-01a14bed-7869-73b3-8435-7b37c5bef6f4 @module-smugglr-core
  Scenario: column order is part of the shape
    Given two places without migratr's ledger whose orders tables have the same columns in a different order
    When the agent runs smugglr sync on them with --json
    Then the record's run status is stopped

  @criterion-01a14bed-7869-73b3-8435-7b4a4a1940ba @module-smugglr-core
  Scenario: an empty target says so
    Given a source with tables and a target with none
    When the agent runs smugglr sync on them with --json
    Then the record says the target has no tables

  @criterion-01a14bed-7869-73b3-8435-7b5a619a66c7 @module-places
  Scenario: smugglr never creates a table
    Given a source with tables and a target with none
    When the agent runs smugglr sync on them
    Then the target still has no tables
