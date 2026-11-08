# Generated from the legion requirement. Do not hand-edit this file.
# FR-SMUGGLR-085 rev 3 holds the --discover scenarios.
Feature: smugglr sync --discover says what two places are and what a sync would meet

  @criterion-01a1b3a8-880c-7682-8bd1-4cdb859dbf81 @module-smugglr
  Scenario: discover describes both places and writes nothing
    Given two places holding tables, a trigger, a view and an index
    When the agent runs smugglr sync on them with --discover
    Then each place's identity is printed first
    And its ledger, tables with rows and estimated bytes, copy order, triggers, views, indexes and ignored tables are printed
    And neither place has changed

  @criterion-01a1b3a8-880c-7682-8bd1-4ce9696c0766 @module-smugglr
  Scenario: a blocking finding comes first and exits 3
    Given a place with two tables that reference each other
    When the agent runs smugglr sync with --discover
    Then the cycle is listed first as blocking
    And the exit code is 3

  @criterion-01a1b3a8-880c-7682-8bd1-4cfc9c1dc1bb @module-smugglr
  Scenario: a D1 place names its account and database
    Given a d1:// place
    When the agent runs smugglr sync with --discover
    Then the identity names the account and the database's uuid

  @criterion-01a1b3a8-880c-7682-8bd1-4d01a79b0520 @module-smugglr
  Scenario: bytes are an estimate from a sample
    Given a place with a table of many rows
    When the agent runs smugglr sync with --discover
    Then that table's bytes are printed as an estimate

  @criterion-01a1b3a8-880c-7682-8bd1-4d139fa2e95e @module-smugglr
  Scenario: the JSON keys a script reads
    Given two places
    When the agent runs smugglr sync with --discover --json
    Then the output has identity, ledger, blocking and shapes_match keys

  @criterion-01a1b3a8-880c-7682-8bd1-4d2f7cc4ca5f @module-smugglr
  Scenario: a stopped sync points to discover
    Given two places whose shapes differ
    When the agent runs smugglr sync on them
    Then the run stops
    And its output names --discover
