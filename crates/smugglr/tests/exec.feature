# Generated from the legion requirement. Do not hand-edit this file.
# FR-SMUGGLR-084 rev 7.
Feature: exec runs migratr's statements on a place, gated by fingerprints both ways (FR-SMUGGLR-084)

  @criterion-01a1b3a8-87b2-7f13-9deb-9e46373e30d0 @module-smugglr
  Scenario: exec runs statements the local database ran
    Given a local database whose ledger records a migration and the hash of its statements
    And a place whose fingerprint is the one expected before that migration
    When migratr calls smugglr exec with those statements
    Then the place has run them
    And exec returns the place's new fingerprint

  @criterion-01a1b3a8-87b2-7f13-9deb-9e5128bc05a9 @module-smugglr
  Scenario: exec refuses statements the local database did not run
    Given a local database whose ledger records a migration and the hash of its statements
    When migratr calls smugglr exec with one byte of those statements changed
    Then the place has not changed
    And exec says the statements are not the ones the local database ran

  @criterion-01a1b3a8-87b2-7f13-9deb-9e6e963fa706 @module-smugglr
  Scenario: exec refuses a place in an unexpected shape
    Given a place with a column added outside migratr
    When migratr calls smugglr exec with statements the local database ran
    Then the place has not changed
    And exec says the place's shape is not the one expected

  @criterion-01a1b3a8-87b2-7f13-9deb-9e7e452f2b3a @module-smugglr
  Scenario: a failing batch leaves the place as it was
    Given statements the local database ran whose last one fails on the place
    When migratr calls smugglr exec with them
    Then the place has not changed

  @criterion-01a1b3a8-87b2-7f13-9deb-9e87841054a9 @module-smugglr
  Scenario: exec reads only schema and ledger
    Given a place
    When migratr calls smugglr exec's read form for the schema and the ledger
    Then it returns them as JSON
    And a read form asking for a table's rows is refused

  @criterion-01a1b3a8-87b2-7f13-9deb-9e921fa94d41 @module-smugglr
  Scenario: help does not name exec
    When the agent runs smugglr --help
    Then exec is not named

  @criterion-01a1b8b3-911f-7873-b069-969235e698fa @module-smugglr
  Scenario: one fingerprint on D1 and on a file
    Given a d1:// place and a local file holding the same schema with a CHECK, a UNIQUE item, a trigger and a view
    When migratr runs exec's read form on each
    Then both fingerprints are equal
    Given the local file's CHECK changed
    When migratr runs exec's read form on it
    Then its fingerprint differs from the place's

  @criterion-01a1b8b3-911f-7873-b069-96a19f850e4c @module-smugglr
  Scenario: an up batch writes the place's ledger row
    Given a local database whose ledger records a migration with its up and down hashes
    When migratr runs exec with that migration's statements on a place file
    Then the place's ledger holds that version with the same up and down hashes

  @criterion-01a1b8b3-911f-7873-b069-96b94ad76f7b @module-smugglr
  Scenario: a down batch needs the local down first and the place's down hash
    Given a place file whose ledger holds a version with its down hash
    And a local database whose ledger no longer holds that version
    When migratr runs exec with the down statements for that version
    Then they run and the place's ledger no longer holds that version
    Given instead a local database whose ledger still holds that version, or a batch with one byte changed
    When migratr runs exec with the down statements
    Then exec refuses and the place has not changed
