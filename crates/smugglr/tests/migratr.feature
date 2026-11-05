# Generated from the legion requirement. Do not hand-edit this file.
Feature: --migratr carries a migration from a local database to a place (FR-SMUGGLR-083)

  @criterion-01a1aa82-c6bb-7679-a8c0-d0ed5f1ef945 @module-smugglr
  Scenario: the first run migrates local and stops for a check
    Given a local database and a place in step
    And a migrations directory holding a migration the local database has not run
    When the agent runs smugglr sync on them with --migratr
    Then the local database has run the migration
    And a snapshot of the local database from before the migration exists
    And the place has not changed
    And the output says to check the local database and run the command again

  @criterion-01a1aa82-c6be-76a0-9488-024ff6210b16 @module-smugglr
  Scenario: the second run carries the migration and the rows
    Given a local database that has run a migration through the first run, with a new row
    And a place that has not run it
    When the agent runs smugglr sync on them with --migratr
    Then the place has run the migration
    And the place's ledger holds the migration
    And the place holds the new row
    And the run says in step

  @criterion-01a1aa82-c6c1-7e6d-9040-51bfe720a1f5 @module-smugglr
  Scenario: a migration that has not run locally is refused
    Given a place whose ledger holds a migration the local ledger lacks
    When the agent runs smugglr sync on them with --migratr
    Then the run stops naming the version
    And the output names migratr up as the only fix
    And neither place has changed

  @criterion-01a1aa82-c6c4-736d-871d-0c1269d9807a @module-smugglr
  Scenario: a place with no ledger and the same shape gets its ledger
    Given a local database that has run its migrations
    And a place with the same tables and rows and no ledger
    When the agent runs smugglr sync on them with --migratr
    Then the place's ledger holds the local ledger's migrations
    And no migration ran on the place
    And the output names the rows it recorded

  @criterion-01a1aa82-c6c7-7d64-8a22-0b348417d281 @module-smugglr
  Scenario: a place with no ledger and another shape stops
    Given a local database that has run its migrations
    And a place with no ledger and a column the local database lacks
    When the agent runs smugglr sync on them with --migratr
    Then the run stops naming the column
    And neither place has changed

  @criterion-01a1aa82-c6ca-705f-8471-9501528f65b8 @module-smugglr
  Scenario: an empty place runs every migration
    Given a local database that has run two migrations and holds rows
    And a place with no tables
    When the agent runs smugglr sync on them with --migratr
    Then the place has run both migrations
    And the place holds the rows
    And the run says in step

  @criterion-01a1aa82-c6cd-7adb-8ebe-0b0172f19f43 @module-smugglr
  Scenario: safe stops when the places are not in step
    Given a local database and a place with a row changed in both
    And a migrations directory holding a migration the local database has not run
    When the agent runs smugglr sync on them with --migratr
    Then the run stops naming the row
    And the local database has not run the migration
    And neither place has changed

  @criterion-01a1aa82-c6d0-7509-989e-403d4e1eda5d @module-smugglr
  Scenario: safe verifies the snapshot against the local database
    Given a local database and a place in step
    And a snapshot that comes out differing from the local database
    When the agent runs smugglr sync on them with --migratr
    Then the run stops saying the snapshot does not match
    And the local database has not run the migration

  @criterion-01a1aa82-c6d3-75d9-9c48-04cb0fae386f @module-smugglr
  Scenario: a rebuild keeps every row and every child
    Given a parent table with a child table with ON DELETE CASCADE, in step on a local database and a place
    And a migration that needs a rebuild of the parent, run on the local database
    When the agent runs smugglr sync on them with --migratr
    Then the place's parent has its new shape
    And the place holds every parent and child row of the local database
    And the local database has lost no row
    And the run says in step

  @criterion-01a1aa82-c6d6-7ac9-be17-812d7ca8ce1a @module-smugglr
  Scenario: a rebuild with the same columns deletes nothing locally
    Given a table in step on a local database and a place
    And a migration that needs a rebuild and leaves the columns as they were, run on the local database
    When the agent runs smugglr sync on them with --migratr
    Then the local database holds every row it held
    And the place holds every one of those rows

  @criterion-01a1aa82-c6d9-7838-8db8-f38159a9c034 @module-smugglr
  Scenario: safe stops a rebuild when the place has moved
    Given a local database that has run a rebuild through the first run
    And a place with a row changed since the snapshot
    When the agent runs smugglr sync on them with --migratr
    Then the run stops naming the row
    And the place still holds that row as changed
    And no table on the place was dropped

  @criterion-01a1aa82-c6dc-709e-997b-442a0670b601 @module-smugglr
  Scenario: safe off skips the checks, still refuses what has not run locally, and deletes no local row
    Given a config with safe set to false under migratr
    And a local database that has run a migration
    And a place that has not
    When the agent runs smugglr sync on them with --migratr
    Then no snapshot is taken
    And the place has run the migration
    And the local database has lost no row

  @criterion-01a1aa82-c6df-764f-9246-2b6852532b1d @module-smugglr
  Scenario: only in step is done
    Given a local database and a place whose rows differ after the migration in a way the sync stops on
    When the agent runs smugglr sync on them with --migratr and --json
    Then the record's run status is not done
    And the output names the rows that differ
    And the output names the snapshot
    And the snapshot still exists

  @criterion-01a1aa82-c6e2-746a-860d-7c05e914452f @module-smugglr
  Scenario: a self-keyed table is carried and synced
    Given a local database and a place with a table keyed by INTEGER PRIMARY KEY and a table keyed by text
    And a migration the local database has run and the place has not
    And a new row on the local database in the table keyed by INTEGER PRIMARY KEY
    When the agent runs smugglr sync on them with --migratr
    Then the place has run the migration
    And the place holds the new row
    And the output names the table as handing out its own keys
    And the run says in step

  @criterion-01a1aa82-c6e5-7aea-a3ad-8b1279658ae8 @module-smugglr
  Scenario: a rebuild refills a self-keyed child
    Given a child table keyed by INTEGER PRIMARY KEY that references a parent
    And a migration that needs a rebuild of the parent, run on the local database
    When the agent runs smugglr sync on them with --migratr
    Then the place holds every parent and child row of the local database
    And the run says in step

  @criterion-01a1aa82-c6e8-70db-9052-bc4e6ac761b2 @module-smugglr
  Scenario: a dry run shows the statements and changes nothing
    Given a local database that has run a migration
    And a place that has not
    When the agent runs smugglr sync on them with --migratr and --dryrun
    Then the output holds the migration's statements
    And neither place has changed

  @criterion-01a1aa9d-989f-7109-a1a2-4dbad1f69b00 @module-smugglr
  Scenario: a local migration with no snapshot stops under safe
    Given a local database that has run a migration through migratr up
    And a place that has not run it
    When the agent runs smugglr sync on them with --migratr
    Then the run stops saying no verified snapshot exists
    And the output names migratr down and safe
    And the place has not changed

  @criterion-01a1ad0f-dcea-74cb-9ee8-150de399372f @module-smugglr
  Scenario: a plain sync after a carry is in step
    Given a local database and a place after a run that carried a migration
    When the agent runs smugglr sync on them without --migratr
    Then the run says in step
    And the exit status is 0

  @criterion-01a1ad0f-dcec-7a36-b78a-6092775a138a @module-smugglr
  Scenario: safe stops any carry when the place has moved
    Given a local database that has run an added column through the first run
    And a place with a row changed since the snapshot
    When the agent runs smugglr sync on them with --migratr
    Then the run stops naming the row
    And the place has not changed

  @criterion-01a1ad0f-dcef-7100-a927-6a214f1da6fa @module-smugglr
  Scenario: an edit made during the check reaches the place
    Given a local database that has run an added column through the first run
    And a row edited on the local database after that run
    When the agent runs smugglr sync on them with --migratr
    Then the place holds the edited row
    And the run says in step
