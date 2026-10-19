# Generated from the legion requirement. Do not hand-edit this file.
Feature: --dryrun, and the record of what moved, and the self-keyed stop (FR-SMUGGLR-063, FR-SMUGGLR-069)

  @criterion-01a14bed-5c0f-7821-a114-9099223a3832 @module-smugglr-core
  Scenario: a dry run writes nothing
    Given two places where the source has rows the target lacks
    When the agent runs smugglr sync on them with --dryrun
    Then neither place has changed

  @criterion-01a14bed-5c0f-7821-a114-90a656c50b9a @module-smugglr-core
  Scenario: a dry run is a preview
    Given two places where the source has rows the target lacks
    When the agent runs smugglr sync on them with --dryrun --json
    Then the record's run status is preview

  @criterion-01a14bed-5c0f-7821-a114-90bde61d7310 @module-smugglr-core
  Scenario: would move
    Given two places where the source's orders table has rows the target lacks
    When the agent runs smugglr sync on them with --dryrun --json
    Then the record gives orders the status would move

  @criterion-01a14bed-5c0f-7821-a114-90c6a6aae56e @module-smugglr-core
  Scenario: moved
    Given two places where the source's orders table has rows the target lacks
    When the agent runs smugglr sync on them with --json
    Then the record gives orders the status moved

  @criterion-01a14bed-5c0f-7821-a114-90d5ecfde439 @module-smugglr-core
  Scenario: in step
    Given two places whose notes table holds the same rows in both
    When the agent runs smugglr sync on them with --json
    Then the record gives notes the status in step

  @criterion-01a14bed-5c0f-7821-a114-90ec404c9b7c @module-smugglr-core
  Scenario: done
    Given two places where the source has rows the target lacks and nothing else differs
    When the agent runs smugglr sync on them with --json
    Then the record's run status is done

  @criterion-01a14bed-5c0f-7821-a114-90f5e6a4c4ef @module-smugglr-core
  Scenario: stopped in part
    Given two places where orders has rows to move and notes has a row changed in both places since the last sync
    When the agent runs smugglr sync on them with --json
    Then the record's run status is stopped in part

  @criterion-01a14bed-5c0f-7821-a114-9107d1d6de20 @module-smugglr-core
  Scenario: the six fields
    Given two places where only new rows move in orders
    When the agent runs smugglr sync on them with --json
    Then the record's entry for orders carries to_target, to_source, deleted_in_target, deleted_in_source, overwritten_in_target and overwritten_in_source

  @criterion-01a14bf0-b502-7a61-8c7b-f6b08b150ecb @module-smugglr-core
  Scenario: new rows appear only in counts
    Given two places where the source's orders table has 41 rows the target lacks
    When the agent runs smugglr sync on them with --json
    Then the record lists no key of those 41 rows

  @criterion-01a14bed-5c0f-7821-a114-91242c0b62c7 @module-smugglr-core
  Scenario: a one-sided edit is not overwritten
    Given two places synced before, and a row of orders since edited in the source only
    When the agent runs smugglr sync on them with --json
    Then the record's overwritten_in_target for orders is empty

  @criterion-01a14bed-5c0f-7821-a114-9139a64f2f05 @module-smugglr-core
  Scenario: a carried delete is named
    Given two places synced before, and a row of orders since deleted in the source
    When the agent runs smugglr sync on them with --json
    Then the record's deleted_in_target for orders holds that row's key

  @criterion-01a14c16-58bf-7c91-b685-dfe1407e0836 @module-smugglr-core
  Scenario: every table stopped
    Given two places whose only differences are rows changed in both places since the last sync
    When the agent runs smugglr sync on them with --json
    Then the record's run status is stopped

  @criterion-01a14bed-75a7-7210-9d31-049332940dd9 @module-smugglr-core
  Scenario: a self-keyed table stops
    Given two places whose sessions table is keyed INTEGER PRIMARY KEY
    When the agent runs smugglr sync on them with --json
    Then the record gives sessions the status stopped with an empty rows list

  @criterion-01a14bed-75a7-7210-9d31-04aaa4d813f4 @module-smugglr-core
  Scenario: the self-keyed stop says why
    Given two places whose sessions table is keyed INTEGER PRIMARY KEY
    When the agent runs smugglr sync on them with --json
    Then the why for sessions says two places would hand out the same key and names a random, unique key such as UUIDv7

  @criterion-01a14bed-75a7-7210-9d31-04b1d70b0df4 @module-smugglr-core
  Scenario: other tables still move
    Given two places whose sessions table is keyed INTEGER PRIMARY KEY and whose orders table has rows to move
    When the agent runs smugglr sync on them with --json
    Then the record gives orders the status moved
