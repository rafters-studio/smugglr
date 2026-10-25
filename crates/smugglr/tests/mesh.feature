# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr mesh is smugglr sync --sync-type mesh (FR-SMUGGLR-078)

  @criterion-01a14bed-95be-7e81-9e8e-2e7689ef819b @module-front-ends
  Scenario: smugglr mesh is smugglr sync --sync-type mesh
    Given one node started with smugglr mesh and another with smugglr sync --sync-type mesh, with the same key file
    When a row is written on the first node's database
    Then the second node holds that row
