# Generated from the legion requirement. Do not hand-edit this file.
Feature: multi keeps a person's own devices in step through smugglr stash and smugglr retrieve (FR-SMUGGLR-077)

  @criterion-01a14bed-9269-7893-82f2-aaf1da3281ad @module-multi
  Scenario: a stash holds what changed since the last stash
    Given a device's database stashed before, and two rows since changed
    When the agent runs smugglr stash on it
    Then the blob holds changes for exactly those two rows

  @criterion-01a14bed-926a-7953-bd04-7901547acd68 @module-multi
  Scenario: a stash carries deletes
    Given a device's database stashed before, and a row since deleted
    When the agent runs smugglr stash on it
    Then the blob holds a delete for that row's key

  @criterion-01a14bed-926a-7953-bd04-791522660147 @module-multi
  Scenario: retrieve applies a blob
    Given a blob stashed from one device, and a second device in step with the first before those changes
    When the agent runs smugglr retrieve on the second device with that blob
    Then the second device holds the same rows as the first

  @criterion-01a14bed-926a-7953-bd04-7927453cb6af @module-multi
  Scenario: blobs in any order
    Given blobs stashed from two devices, each holding changes to rows the other also changed later
    When a third device retrieves the blobs in the opposite order to how they were stashed
    Then the third device holds the same rows as the first two

  @criterion-01a14bed-926a-7953-bd04-79386c1aed3c @module-multi
  Scenario: a blob of another shape stops
    Given a blob whose header's shape stamp for orders differs from the device's orders table
    When the agent runs smugglr retrieve on the device with that blob
    Then no change of the blob is applied

  @criterion-01a14bf0-f2d3-7901-b361-0b15196dbcda @module-multi
  Scenario: retrieve says when the device is done with a blob
    Given a blob whose every change can be applied on the device
    When the app calls retrieve with that blob
    Then the record's status is done

  @criterion-01a14bed-926a-7953-bd04-7959049575d8 @module-multi
  Scenario: a device away past the sunset catches up
    Given a device offline for longer than the sunset while the others changed rows
    When it retrieves a stash made from a device that stayed in step
    Then it holds the same rows as the others

  @criterion-01a14bed-926a-7953-bd04-7962a67e508a @module-multi
  Scenario: moving a whole database
    Given a place with rows, and a new place with the same tables and no rows
    When the agent runs smugglr stash on the old place and smugglr retrieve on the new place with that stash
    Then the new place holds every row of the old one

  @criterion-01a14c16-8850-7433-90d3-21642a072ec8 @module-multi
  Scenario: retrieve says who still needs the blob
    Given a blob stashed for three devices, already crossed off by one
    When a second device runs smugglr retrieve of that blob with --json
    Then the record names the third device as the target that remains

  @criterion-01a14c16-8850-7433-90d3-217c768902e8 @module-multi
  Scenario: moving between hosted services
    Given an HTTP-SQL service holding rows, and a second HTTP-SQL service with the same shape and no rows
    When the agent runs smugglr stash from the first and smugglr retrieve into the second
    Then the second holds every row of the first
