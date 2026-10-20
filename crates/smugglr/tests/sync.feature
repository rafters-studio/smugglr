# Generated from the legion requirement. Do not hand-edit this file.
Feature: one sync between two places, with places and memory as they are

  @criterion-01a14bed-7cc2-7a31-be37-6083dd3bd3e7 @module-places
  Scenario: anyone can write an adapter
    Given an adapter written outside smugglr for a service smugglr does not know, and a local file with rows that service lacks
    When a program syncs the file with that service through the adapter
    Then the service holds the same rows as the file

  @criterion-01a14bed-813e-7e03-9baf-8896024303dc @module-memory
  Scenario: a row committed mid-run is kept
    Given two places with no steering column, and a row committed into the source while a run is reading
    When that run ends and the agent runs smugglr sync on them again
    Then the target holds that row

  @criterion-01a14bed-813e-7e03-9baf-88ad4ffb34a7 @module-memory
  Scenario: a clock behind loses nothing
    Given two places synced before, the target's clock 60 seconds behind, and a row since added to the target
    When the agent runs smugglr sync on them
    Then the source holds that row

  @criterion-01a14bed-813e-7e03-9baf-88b66831671e @module-memory
  Scenario: no marks in a place
    Given two places with rows to move
    When the agent runs smugglr sync on them
    Then each place's sqlite_master, user_version and application_id are as they were before the run

  @criterion-01a14bed-813e-7e03-9baf-88c699476f1b @module-memory
  Scenario: one folder per pair
    Given a local file synced with two different places, one sync each
    When the agent looks in smugglr's config directory
    Then it holds one folder for each of the two pairs

  @criterion-01a14bed-60a6-78b2-99ee-8f0c7ca5bc64 @module-places
  Scenario: a read-back that does not match is not done
    Given a target that keeps a written row differently from how it was written
    When the agent runs smugglr sync on the source and that target
    Then smugglr exits 4

  @criterion-01a14bed-5c0f-7821-a114-9144e7a109e5 @module-memory
  Scenario: nothing of the record is kept
    Given two places where the source has rows the target lacks
    When the agent runs smugglr sync on them and the run is done
    Then smugglr's own files hold no copy of the run's record

  @criterion-01a14bed-8499-7e32-9e9a-8361d15d3e83 @module-memory
  Scenario: a delete after a cut stays deleted
    Given a run that copied a new row from the source to the target and was cut off before it ended
    When the row is deleted in the source and the agent runs smugglr sync on them again
    Then the row is gone from both places

  @criterion-01a14bed-8499-7e32-9e9a-83759a7c5dd5 @module-memory
  Scenario: the next run finishes the cut-off run
    Given a run between two places cut off part-way through writing
    When the agent runs smugglr sync on them again
    Then both places hold the same rows

  @criterion-01a14bed-8499-7e32-9e9a-838d0ee4a387 @module-memory
  Scenario: a row changed since the cut stops
    Given a run cut off before writing a row of notes to the target, and that row since edited in the target
    When the agent runs smugglr sync on them again with --json
    Then the record names that row's key as stopped

  @criterion-01a14bed-8499-7e32-9e9a-839f719641bc @module-memory
  Scenario: the journal goes when the run is done
    Given two places with rows to move
    When the agent runs smugglr sync on them and the run is done
    Then smugglr's files hold no smugglr.sync journal
