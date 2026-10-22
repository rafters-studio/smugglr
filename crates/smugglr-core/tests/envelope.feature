# Generated from the legion requirement. Do not hand-edit this file.
Feature: the signed envelope multi and the mesh carry each Change in, its sunset and the graveyard (FR-SMUGGLR-075, FR-SMUGGLR-076)

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

  @criterion-01a14bed-8f07-7733-a156-71fb273f8cf0 @module-smugglr-core
  Scenario: an early delete keeps the row deleted
    Given a row inserted on one node and deleted on another
    When a third place receives the delete before the insert, inside the graveyard window
    Then the row is absent at the third place

  @criterion-01a14bed-8f07-7733-a156-720873663ba7 @module-smugglr-core
  Scenario: insert, edit, delete with the delete first
    Given a row inserted, then edited, then deleted on other nodes
    When a place receives the delete first, then the insert, then the edit, inside the graveyard window
    Then the row is absent at that place

  @criterion-01a14bed-8f07-7733-a156-721eff97fc9b @module-smugglr-core
  Scenario: a key re-created inside the window is dropped and named
    Given a row deleted on every node, and then re-created with the same key on one node inside the graveyard window
    When the other places receive the insert
    Then the row is absent at the other places
    And it stays where it was made
    And the record at the other places names it under dropped

  @criterion-01a14bed-8f07-7733-a156-7223f898d11d @module-smugglr-core
  Scenario: an insert after the window comes back
    Given a row deleted at a place longer ago than the graveyard window
    When the place receives an insert for that key
    Then the place holds the row

  @criterion-01a14bed-8f07-7733-a156-723b8b4a8e43 @module-smugglr-core
  Scenario: the graveyard window defaults to 12 hours
    Given no graveyard setting, and a row deleted at a place less than 12 hours ago
    When the place receives an insert for that key
    Then the row is absent at the place

  @criterion-01a14bed-8f07-7733-a156-7248df4e74eb @module-smugglr-core
  Scenario: a dead change is ignored
    Given a change whose sunset has passed
    When a place receives it
    Then the place's row is unchanged

  @criterion-01a14bed-8f07-7733-a156-725870c0e2db @module-smugglr-core
  Scenario: a waiting change ends at its sunset
    Given a place holding a change whose from matches nothing there
    When the change's sunset passes
    Then the place holds no waiting change

  @criterion-01a14bed-8f07-7733-a156-726cbde362f5 @module-smugglr-core
  Scenario: a late edit to a deleted row is ignored and logged
    Given a row deleted on one device, and an edit to it made on another device
    When a place receives the edit past its sunset, after the row's tombstone has gone
    Then the place ignores the edit
    And its dropped log holds the edit
    And the record names the edit by its change hash

  @criterion-01a14bed-8f07-7733-a156-7271350ba4df @module-smugglr-core
  Scenario: a replayed late edit to a deleted row stops
    Given a place whose dropped log holds an edit to a row deleted elsewhere
    When the agent replays it with --replay and its change hash, under --sync-mode stop
    Then it stops as edit versus delete
    And the record names the row

  @criterion-01a14c16-8548-7450-a180-5ddc2813fa39 @module-smugglr-core
  Scenario: an edit to a deleted row is a conflict, not a drop
    Given a row deleted on one device, and on another device an edit made to that row before it heard of the delete
    When the edit's change arrives on the first device under --sync-mode stop
    Then the row stops and is named by key

  @criterion-01a14c16-8548-7450-a180-5de11d14d3df @module-smugglr-core
  Scenario: a dropped change is named
    Given a change waiting on a device for the change it built on, which never arrives
    When the waiting change's sunset passes and the device next retrieves
    Then the record names that change's table and key under dropped, with the reason that its sunset passed

  @criterion-01a150da-3bf1-7892-a858-85b9a265d175 @module-smugglr-core
  Scenario: every dropped change is logged and named
    Given a place that drops a change
    When the run ends
    Then the dropped log holds the change's envelope and values
    And the record names it under dropped with its table, key, reason and change hash

  @criterion-01a1638c-55d7-7623-b420-fccf1a159557 @module-smugglr-core
  Scenario: a replay overrides the graveyard
    Given a place whose dropped log holds an insert dropped because its key's tombstone was live
    When the agent replays it with its change hash while that tombstone is still live
    Then the place holds the inserted row
    And the record does not name it under dropped

  @criterion-01a16446-b84a-7853-ab1a-00295cf52e6f @module-smugglr-core
  Scenario: a late copy the row moved past is skipped
    Given a place that applied a change f to a, then a change a to b, for one row
    When a copy of the change f to a arrives again
    Then the place skips it and the row stays at b
