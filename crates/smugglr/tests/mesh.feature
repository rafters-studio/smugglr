# Generated from the legion requirement. Do not hand-edit this file.
Feature: smugglr mesh as a command: its front ends, its node's name and port, its key file, and a flood (FR-SMUGGLR-078)

  @criterion-01a14bed-95be-7e81-9e8e-2e7689ef819b @module-front-ends
  Scenario: smugglr mesh is smugglr sync --sync-type mesh
    Given one node started with smugglr mesh and another with smugglr sync --sync-type mesh, with the same key file
    When a row is written on the first node's database
    Then the second node holds that row

  @criterion-01a17503-1c41-7da1-bfbe-c356defeddf0 @module-mesh
  Scenario: a node is named by its machine
    Given a node running smugglr mesh on a machine named puck
    When a row is written on puck's database
    Then the change the other nodes receive names puck as its node

  @criterion-01a17503-1c41-7da1-bfbe-c3603bc1e06c @module-mesh
  Scenario: the mesh port is a setting
    Given one node with no port setting and one node with port set to 40000
    When each node starts smugglr mesh
    Then the first sends and listens on UDP port 31337
    And the second sends and listens on UDP port 40000

  @criterion-01a175e5-f67e-7e00-87b3-8096ec5c0eda @module-mesh
  Scenario: a key file others can read is refused
    Given a key file readable by its group
    When a node starts smugglr mesh with it
    Then the node does not start
    And it says chmod 600 on that key file fixes it

  @criterion-01a175e5-f67e-7e00-87b3-80a0a207fff3 @module-mesh
  Scenario: keygen makes a key, and appending one rotates it
    Given a key file written by smugglr mesh keygen and shared by two nodes
    When smugglr mesh keygen appends a second key on the first node only
    Then the first node's changes are signed with the newer key
    And the first node still accepts a change the second node signs with the older key

  @criterion-01a175e5-f67e-7e00-87b3-80b1181b1682 @module-mesh
  Scenario: a key never appears in output
    Given two nodes running smugglr mesh with one key file
    When a row is written and a change signed with an unknown key arrives
    Then neither node's output or errors contain the key
    And the record names the key only by its key_id

  @criterion-01a17646-71f8-7dd0-8860-1e64e558f4b7 @module-mesh
  Scenario: a flood is paced, not dropped
    Given two nodes running smugglr mesh
    When 500,000 rows are written at once on the first node
    Then the second node holds all 500,000 once acknowledgements stop
    And neither node's socket reports a datagram dropped for a full buffer

  @criterion-01a17646-71f8-7dd0-8860-1e73cea8e31b @module-mesh
  Scenario: received changes are applied in batches
    Given two nodes running smugglr mesh
    When 50,000 rows are written at once on the first node
    Then the second node applies them in transactions of at most 20,000 changes, not one per change
