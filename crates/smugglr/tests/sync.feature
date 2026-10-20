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

  @criterion-01a14bed-7cc2-7a31-be37-607bc718af88 @module-places
  Scenario: a local file and an HTTP-SQL service
    Given a local file and a service that speaks HTTP-SQL, where the file has rows the service lacks
    When the agent runs smugglr sync on them
    Then the service holds the same rows as the file

  @criterion-01a14bed-7cc2-7a31-be37-60931d9b1352 @module-places
  Scenario: big integers arrive whole
    Given a service that speaks HTTP-SQL holding a row with the integer 9007199254740993, and a local file without it
    When the agent runs smugglr sync on the service and the file
    Then the file's row holds 9007199254740993

  @criterion-01a14bed-7cc2-7a31-be37-60a4293b3636 @module-places
  Scenario: the largest integer arrives whole
    Given a service that speaks HTTP-SQL holding a row with the integer 9223372036854775807, and a local file without it
    When the agent runs smugglr sync on the service and the file
    Then the file's row holds 9223372036854775807

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

  @criterion-01a14bed-72a8-7b31-abbd-3d0d1302b90c @module-smugglr-core
  Scenario: the error comes before any write
    Given two places with rows to move, whose orders table has no steering column named in the config
    When the agent runs smugglr sync on them with --sync-mode last
    Then neither place has changed

  @criterion-01a14bed-5112-7002-b72f-4272b004411c @module-front-ends
  Scenario: a stop is a record
    Given two places with a row changed in both since the last sync
    When a program syncs them through the Rust crates
    Then the call returns a record whose table status for that row's table is stopped

  @criterion-01a14bed-5112-7002-b72f-4283f84d3e2e @module-front-ends
  Scenario: an unreachable place is an error
    Given a local file and an HTTP-SQL address that does not answer
    When a program syncs them through the Rust crates
    Then the call returns an error

  @criterion-01a14bed-5112-7002-b72f-429ae7829fae @module-front-ends
  Scenario: dryrun in the options
    Given two places where the source has rows the target lacks
    When a program syncs them through the Rust crates with dryrun set
    Then the call returns a record whose run status is preview

  @criterion-01a14bed-4d5a-7dc2-b311-edda0371ac87 @module-smugglr-core
  Scenario: an exit-4 record names what was written
    Given a target that drops its connection after the run has written some rows
    When the agent runs smugglr sync --json on the source and that target
    Then the record names the tables and keys of the rows written before the connection dropped
