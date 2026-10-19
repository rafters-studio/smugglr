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
