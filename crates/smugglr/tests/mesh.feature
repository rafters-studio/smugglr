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

  @criterion-01a176cc-730a-70c0-a641-dce0f9d2d785 @module-mesh
  Scenario: first sends are unicast and a lost copy is resent at once
    Given two nodes running smugglr mesh that have heard each other
    When 50,000 rows are written at once on the first node, and the relay drops one datagram in ten
    Then the first node sends each change's first copy unicast to the second node
    And each lost copy is applied on the second node within one second of its loss
    And resends never take more than a quarter of what the first node sends

  @criterion-01a176cc-730a-70c0-a641-dcff52054467 @module-mesh
  Scenario: the window shrinks on congestion, not on a lost datagram
    Given two nodes running smugglr mesh, and a relay that drops one datagram in ten
    When 50,000 rows are written at once on the first node
    Then the first node's send window never halves for a lost datagram

  @criterion-01a176d8-f51b-73b0-b21e-f433d87c812f @module-mesh
  Scenario: peers named in config keep in step without broadcast
    Given two nodes running smugglr mesh on a relay that passes no broadcast, each naming the other in its peers setting
    When a row is written on the first node
    Then the second node holds that row

  @criterion-01a17782-0bc1-7e61-8719-715911ed27bc @module-mesh
  Scenario: repair continues after the first sends run out
    Given two nodes running smugglr mesh, and a relay that drops one datagram in ten
    When 50,000 rows are written at once on the first node and every first copy has been sent
    Then the first node keeps resending until the second node holds all 50,000 rows

  @criterion-01a17782-0bc1-7e61-8719-71661f5e4c21 @module-mesh
  Scenario: a burst's last change is repaired like its first
    Given two nodes running smugglr mesh with the backpressure setting at 1 MB, and a relay that drops one datagram in ten
    When 50,000 rows are written at once on the first node
    Then the second node holds the last row written within one second of its latest loss

  @criterion-01a17782-0bc1-7e61-8719-71785d97eff3 @module-mesh
  Scenario: a node back from past the sunset says how to catch up
    Given a node whose last run ended longer ago than the sunset
    When it starts smugglr mesh
    Then it prints that it has been away longer than the sunset
    And it names smugglr stash and smugglr retrieve as the way to catch up
    And it keeps running and applies new changes
