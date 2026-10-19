# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr sync on the command line

  @criterion-01a14bed-7cc2-7a31-be37-60b3a9434e90 @module-front-ends
  Scenario: another engine is refused
    Given a local file and a postgres:// address
    When the agent runs smugglr sync on them
    Then smugglr refuses, saying smugglr keeps SQLite in step with SQLite

  @criterion-01a14bed-5c0f-7821-a114-9159a7083943 @module-front-ends
  Scenario: the record as text
    Given two places where the source's orders table has rows the target lacks
    When the agent runs smugglr sync on them without --json
    Then the output names orders with its status and counts in text

  @criterion-01a14bed-4909-7023-8f5a-6aa060849495 @module-front-ends
  Scenario: --table limits a run to the named table
    Given two places whose notes and orders tables both have rows to move
    When the agent runs smugglr sync on them with --table notes
    Then no row of orders is written in either place

  @criterion-01a14bed-4909-7023-8f5a-6ab8f82913b4 @module-front-ends
  Scenario: --table repeats
    Given two places whose notes, orders and line_items tables all have rows to move
    When the agent runs smugglr sync on them with --table notes --table orders
    Then the record lists notes and orders and no other table

  @criterion-01a14bed-4909-7023-8f5a-6acac6b87d0e @module-front-ends
  Scenario: direct is the default for two places
    Given two copies of the same pair of places, with the same rows changed in each copy
    When the agent syncs one copy with no --sync-type and the other with --sync-type direct
    Then the two records hold the same statuses, counts and keys

  @criterion-01a14bed-4909-7023-8f5a-6ad815d86866 @module-front-ends
  Scenario: --watch prints one record per run
    Given two places
    When the agent runs smugglr sync on them with --watch --json and stops it after several intervals
    Then smugglr printed one record for each run it made

  @criterion-01a14bed-4909-7023-8f5a-6aed63e21762 @module-front-ends
  Scenario: a sync is one run
    Given two places
    When the agent runs smugglr sync on them without --watch
    Then smugglr exits after printing one record

  @criterion-01a14c16-47db-7b91-9033-a85b36d79732 @module-front-ends
  Scenario: --watch defaults to 60 seconds
    Given two places
    When the agent runs smugglr sync on them with --watch and no number, and stops it after three minutes
    Then smugglr made a run about every 60 seconds

  @criterion-01a14be5-057d-7163-9138-90702b17b1dd @module-front-ends
  Scenario: the Rust crates and the CLI give the same record
    Given two copies of the same pair of places, with the same rows changed in each copy
    When the agent syncs one copy with smugglr sync --json and a program syncs the other through the Rust crates
    Then the two records hold the same statuses, counts and keys

  @criterion-01a14bed-6a79-7543-91fd-005d8c496793 @module-front-ends
  Scenario: no ignore flag
    Given the smugglr CLI
    When the agent runs smugglr sync --help
    Then the help lists no flag for ignoring tables

  @criterion-01a14bed-657d-7353-b2bb-4507f28693ab @module-front-ends
  Scenario: too much for one sync exits 5
    Given two places never synced with each other, holding more than the max_wire setting allows
    When the agent runs smugglr sync on them
    Then smugglr exits 5

  @criterion-01a14bed-72a8-7b31-abbd-3ce1b2645982 @module-front-ends
  Scenario: last without a steering column is a usage error
    Given two places whose orders and line_items tables have no steering column named in the config
    When the agent runs smugglr sync on them with --sync-mode last
    Then smugglr exits 2

  @criterion-01a14bed-72a8-7b31-abbd-3cf53995f701 @module-front-ends
  Scenario: the error lists every table without one
    Given two places whose orders and line_items tables have no steering column named in the config
    When the agent runs smugglr sync on them with --sync-mode last
    Then one error names both orders and line_items
