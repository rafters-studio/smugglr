# Generated from the legion requirement. Do not hand-edit this file.
Feature: the Change and the one decision path (FR-SMUGGLR-075)

  @criterion-01a14bed-8b9b-7142-a602-8259446821d2 @module-smugglr-core
  Scenario: from matches, the change applies
    Given a place holding a row whose hash is the from of an arriving change
    When the place receives the change
    Then the row holds the change's values

  @criterion-01a14bed-8b9b-7142-a602-826a1c1b0307 @module-smugglr-core
  Scenario: a repeat is skipped
    Given a place holding a row whose hash is already the to of an arriving change
    When the place receives the change
    Then the row is unchanged
