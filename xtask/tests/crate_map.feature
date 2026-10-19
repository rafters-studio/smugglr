# Generated from the legion requirement. Do not hand-edit this file.
Feature: the crates follow the crate map (NFR-SMUGGLR-009)

  @criterion-01a150d0-e1f8-7dc1-ad61-a90e5fd64a79
  Scenario: CI fails a change that adds a dependency between workspace crates the crate map does not allow, naming both crates
    Given the workspace as it is
    Then the crate map holds
    When a change makes smugglr-core depend on the workspace crate smugglr-memory
    Then the crate map check fails naming smugglr-core and smugglr-memory

  @criterion-01a150d0-e1f8-7dc1-ad61-a91ab67285bb
  Scenario: smugglr-core builds for wasm32-unknown-unknown and depends on no crate that does I/O or reads a clock
    Given the workspace as it is
    Then smugglr-core builds for wasm32-unknown-unknown
    And smugglr-core depends on no crate that does I/O or reads a clock
    When a change makes smugglr-core depend on the external crate tokio
    Then the crate map check fails naming smugglr-core and tokio
