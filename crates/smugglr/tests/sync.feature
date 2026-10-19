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

  @criterion-01a14bed-657d-7353-b2bb-44d67ce1d3ff @module-smugglr-core
  Scenario: a first sync says so
    Given two places never synced with each other
    When the agent runs smugglr sync on them with --json
    Then the record says first_sync true

  @criterion-01a14bed-657d-7353-b2bb-44ec8f7addcc @module-smugglr-core
  Scenario: a first sync cannot see deletes
    Given two places never synced with each other
    When the agent runs smugglr sync on them with --json
    Then the record says deletes_seen false

  @criterion-01a14bed-657d-7353-b2bb-44f3b5dfb317 @module-smugglr-core
  Scenario: a first sync copies one-sided rows
    Given two places never synced with each other, and a row only in the target
    When the agent runs smugglr sync on them
    Then the source holds that row

  @criterion-01a14bed-657d-7353-b2bb-4513d83aa4d5 @module-smugglr-core
  Scenario: too much for one sync moves nothing
    Given two places never synced with each other, holding more than the max_wire setting allows
    When the agent runs smugglr sync on them
    Then neither place has changed

  @criterion-01a14bed-657d-7353-b2bb-4529a881ff60 @module-smugglr-core
  Scenario: too much for one sync says what to do
    Given two places never synced with each other, holding more than the max_wire setting allows
    When the agent runs smugglr sync on them
    Then the output tells the agent to move it with smugglr stash and then smugglr retrieve

  @criterion-01a14bed-657d-7353-b2bb-45335407d68c @module-smugglr-core
  Scenario: a dry run shows the weight
    Given two places never synced with each other
    When the agent runs smugglr sync on them with --dryrun
    Then the output gives the rows, bytes and tables the first sync would move

  @criterion-01a14c16-5feb-74e2-b96d-6f2f229c4554 @module-smugglr-core
  Scenario: the default limit is 100 MB
    Given no max_wire setting, and two places never synced with each other whose rows weigh more than 100 MB
    When the agent runs smugglr sync on them
    Then smugglr exits 5
