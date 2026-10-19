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
