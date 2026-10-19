# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr binary

  @smoke
  Scenario: the binary reports its name and version
    When smugglr is run with no arguments
    Then it prints its name and version
