# Generated from the legion requirement. Do not hand-edit this file.
Feature: the signed envelope multi and the mesh carry each Change in (FR-SMUGGLR-075)

  @criterion-01a14bed-8b9b-7142-a602-827598740c04 @module-smugglr-core
  Scenario: a change waits for the one before it
    Given a place that missed a change to a row, and then receives the next change to that row
    When the missed change arrives
    Then the row holds the values of the later change

  @criterion-01a14bed-8b9b-7142-a602-828f43bee50a @module-smugglr-core
  Scenario: two changes from one from stop
    Given two nodes that each changed the same row before hearing the other
    When each node receives the other's change
    Then each node names that row as stopped

  @criterion-01a14bed-8b9b-7142-a602-829b6116b3dc @module-smugglr-core
  Scenario: last settles two changes from one from
    Given a config naming updated_at as the steering column, and two nodes that each changed the same row before hearing the other, one with a later updated_at
    When each node receives the other's change under --sync-mode last
    Then the row on both nodes holds the values of the change with the later updated_at

  @criterion-01a14bed-8b9b-7142-a602-82adf68014ba @module-smugglr-core
  Scenario: a transaction applies all or none
    Given a transaction of several changes, one of which cannot be applied here
    When the place receives the transaction
    Then none of its changes is applied

  @criterion-01a14bed-8b9b-7142-a602-82bb0da58d92 @module-smugglr-core
  Scenario: a tampered envelope is refused
    Given a change whose envelope was altered after it was signed
    When a place receives the change
    Then the row is unchanged

  @criterion-01a14bed-8b9b-7142-a602-82c67fa3a0e4 @module-smugglr-core
  Scenario: values that do not match to are refused
    Given a change whose values do not hash to the envelope's to
    When a place receives the change
    Then the row is unchanged

  @criterion-01a14c1a-643f-75f3-830d-ea50923ff560 @module-smugglr-core
  Scenario: a conflict seen at a third place
    Given three nodes, two of which change the same row before hearing each other, and a third that has applied the first change
    When the second change reaches the third node under --sync-mode stop
    Then the third node stops that row and names it by key
