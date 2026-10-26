# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr mesh, a local network where nobody is in charge, sends each change as it happens, signed and encrypted with one shared key, and resends it until every other node has it (FR-SMUGGLR-078)

  @criterion-01a14bed-95be-7e81-9e8e-2df3c09a5c01 @module-mesh
  Scenario: changes go out as they happen
    Given three nodes running smugglr mesh with the same key file
    When a row is written on the first node's database
    Then the other two nodes hold that row without anyone running a sync

  @criterion-01a14bed-95be-7e81-9e8e-2e04e577f5f6 @module-mesh
  Scenario: a node finds its own changes
    Given two nodes running smugglr mesh with the same key file
    When a program other than smugglr edits a row in the first node's database
    Then the second node holds the edited row

  @criterion-01a14bed-95be-7e81-9e8e-2e1e82d09755 @module-mesh
  Scenario: a lost change is resent
    Given three nodes running smugglr mesh, and the first send of a change to the third node lost
    When the sender's resend reaches the third node
    Then the third node holds the change

  @criterion-01a14bed-95be-7e81-9e8e-2e2f72589894 @module-mesh
  Scenario: resends end at the sunset
    Given a node that has not acknowledged a change
    When the change's sunset passes
    Then the sender no longer sends that change

  @criterion-01a14bed-95be-7e81-9e8e-2e3828afa5a0 @module-mesh
  Scenario: a change from outside the mesh is refused
    Given a node running smugglr mesh with one key file
    When it receives a change signed with a different key
    Then the row is unchanged

  @criterion-01a14bed-95be-7e81-9e8e-2e4ddb4c827d @module-mesh
  Scenario: the previous key is accepted until its last change's sunset
    Given a node that holds a newer key and the previous key, whose last change's sunset has not passed
    When it receives a change signed with the previous key
    Then the change is applied

  @criterion-01a14bed-95be-7e81-9e8e-2e5e23d00d4e @module-mesh
  Scenario: sign with the newest key
    Given a node holding the previous key and a newer key
    When it sends a change
    Then the change's key_id names the newer key

  @criterion-01a14bed-95be-7e81-9e8e-2e66ca3345df @module-mesh
  Scenario: nobody leaving stops the rest
    Given three nodes running smugglr mesh, and the first node gone
    When a row is written on the second node's database
    Then the third node holds that row

  @criterion-01a14c16-8b0b-7c22-94df-fb3f24cf2bd8 @module-mesh
  Scenario: nothing in the clear
    Given two mesh nodes sharing a key, and a capture of the traffic between them
    When a row holding a known value is changed on one node
    Then the capture holds neither that value nor that row's key in the clear

  @criterion-01a17646-71f8-7dd0-8860-1e498d7b5f0c @module-mesh
  Scenario: the outbox survives a restart
    Given two nodes running smugglr mesh, and the second node stopped
    When a row is written on the first node, and the first node is stopped and started again
    And the second node starts again
    Then the second node holds the row
    And the user's database holds no table smugglr made

  @criterion-01a17646-71f8-7dd0-8860-1e587ee0d5d8 @module-mesh
  Scenario: back-pressure stops the outbox at its line
    Given a node with backpressure set to 1 MB whose only peer is asleep
    When 5 MB of rows are written on the node
    Then the node's outbox holds no more than 1 MB
    And once the peer wakes and acknowledges, the node takes the remaining rows and the peer ends with them all
