# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr decides what changed from each key's last hash per pair of places, kept in its own files; it leaves no marks in any place (FR-SMUGGLR-072)

  @criterion-01a14bed-813e-7e03-9baf-88d5ab566767 @module-memory
  Scenario: a tab's memory stays beside its database
    Given a tab opened with @smugglr/web that has synced with its endpoint
    When the app lists the tables of its database
    Then it finds only the tables the app created, and the tab's memory is in the tab's own OPFS storage, outside the app's database
