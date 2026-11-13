# Generated from the legion requirement. Do not hand-edit this file.
Feature: a table without a primary key syncs like any other table (FR-SMUGGLR-086)

  @criterion-01a1bf89-43b0-7282-b1af-366bc8062766 @module-smugglr
  Scenario: a keyless table syncs both ways
    Given two place files each holding a table notes with no primary key, with different rows
    When the agent runs smugglr sync on them
    Then each place holds both sides' rows
    And a second sync moves nothing

  @criterion-01a1bf89-43b0-7282-b1af-3676c82e133a @module-smugglr
  Scenario: a changed keyless row arrives as that row
    Given two place files in step holding a keyless table notes
    And one side changes a row's values
    When the agent runs smugglr sync on them
    Then the other side holds the changed row and not the old one

  @criterion-01a1bf89-43b0-7282-b1af-368037e5397e @module-smugglr
  Scenario: identical keyless rows are each kept
    Given a place file whose keyless table notes holds two identical rows, and an empty place file with the same table
    When the agent runs smugglr sync on them
    Then the empty place holds two identical rows

  @criterion-01a1bf89-43b0-7282-b1af-369f08a8b4a3 @module-smugglr
  Scenario: a full-text table syncs its rows
    Given two place files each holding an FTS5 table docs, one with two rows
    When the agent runs smugglr sync on them
    Then a full-text MATCH on the other place finds both rows
    And no shadow table of docs was written by the sync

  @criterion-01a1bf89-43b0-7282-b1af-36a00e56b9a0 @module-smugglr
  Scenario: AUTOINCREMENT stops the sync
    Given a place file with a table using AUTOINCREMENT
    When the agent runs smugglr sync on it
    Then the sync stops before anything is written
    And the output names the table
