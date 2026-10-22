# Generated from the legion requirement. Do not hand-edit this file.
Feature: @smugglr/web is a tab's own database, kept in step by the engine in the tab with an endpoint the app names (FR-SMUGGLR-062)

  @criterion-01a1631e-69ee-71d1-be68-5cf87923e742 @module-front-ends
  Scenario: a first sync fills an empty tab
    Given a stand-in endpoint holding rows in two tables
    When the app opens a tab with @smugglr/web naming that endpoint
    Then once open() finishes, the tab's database holds every row the endpoint holds

  @criterion-01a1631e-69ee-71d1-be68-5d0baf6528e8 @module-front-ends
  Scenario: a cleared tab rebuilds
    Given a tab in step with a stand-in endpoint
    When the tab's data is cleared and the app opens the tab again
    Then the tab's database holds every row the endpoint holds

  @criterion-01a1631e-69ee-71d1-be68-5d1fc1686141 @module-front-ends
  Scenario: a cut-off first sync finishes
    Given a stand-in endpoint that drops the connection partway through a tab's first sync
    When the app opens the tab again with the connection whole
    Then the tab's database holds every row the endpoint holds, each once

  @criterion-01a1631e-69ee-71d1-be68-5d2e6ffb76f4 @module-front-ends
  Scenario: a row written in the tab reaches the endpoint
    Given a tab in step with a stand-in endpoint
    When the app inserts a row into the tab's database and calls sync()
    Then the endpoint holds that row

  @criterion-01a1631e-69ee-71d1-be68-5d3cfe1e3eaa @module-front-ends
  Scenario: a row changed at the endpoint reaches the tab
    Given a tab in step with a stand-in endpoint
    When a row is changed at the endpoint and the app calls sync()
    Then the tab's database holds the changed row

  @criterion-01a1631e-69ee-71d1-be68-5d40aecf33b4 @module-front-ends
  Scenario: deletes travel both ways
    Given a tab in step with a stand-in endpoint
    When the app deletes one row in the tab, another row is deleted at the endpoint, and the app calls sync()
    Then neither row is in the tab's database or at the endpoint

  @criterion-01a1631e-69ee-71d1-be68-5d5ea9cd1aed @module-front-ends
  Scenario: no sync without the app
    Given a tab whose first sync has finished
    When 60 seconds pass with the app calling nothing
    Then the stand-in endpoint has recorded no request since the first sync

  @criterion-01a1631e-69ee-71d1-be68-5d6cead5c5c0 @module-front-ends
  Scenario: no SQL on the wire
    Given a tab opened with @smugglr/web naming a stand-in endpoint
    When the tab runs a first sync and the app writes a row and calls sync()
    Then every request the endpoint recorded names one of tables, hashes, rows, weights or write, and none carries SQL

  @criterion-01a1631e-69ee-71d1-be68-5d787c58d58f @module-front-ends
  Scenario: two tabs sync one after the other
    Given two tabs of one origin, both in step with a stand-in endpoint, each with a row of its own written
    When both tabs call sync() at the same moment
    Then every request of one tab's sync was recorded before the first request of the other's, and both tabs hold both rows

  @criterion-01a1631e-69ee-71d1-be68-5d8ddc1393d4 @module-front-ends
  Scenario: table-changed reaches every tab
    Given two open tabs of one origin, in step with a stand-in endpoint
    When a row is changed at the endpoint and one tab calls sync(), and the app then writes a row itself
    Then both tabs received table-changed naming that table and the changed row's key, and none for the app's own write

  @criterion-01a1631e-69ee-71d1-be68-5d9d22265e18 @module-front-ends
  Scenario: a row changed in both places stops its table
    Given a tab in step with a stand-in endpoint
    When one row is changed in the tab and differently at the endpoint, and the app calls sync()
    Then the record sync() returns shows that table stopped and names the row

  @criterion-01a1631e-69ee-71d1-be68-5da631fd18c1 @module-front-ends
  Scenario: a first sync over max_wire writes nothing
    Given a stand-in endpoint holding more than max_wire
    When the app opens a tab naming that endpoint
    Then the tab's database holds no row, and the record says to fill the tab by retrieve

  @criterion-01a1631e-69ee-71d1-be68-5db9ffa6f21b @module-front-ends
  Scenario: retrieve then sync fills an empty tab
    Given a stash blob made from a stand-in endpoint, and a row changed at the endpoint after the stash
    When the app opens an empty tab, calls retrieve() with the blob, then calls sync()
    Then the tab's database holds every row the endpoint holds, including the changed row

  @criterion-01a16322-b73a-7350-8be7-c3ebe9860dd5 @module-front-ends
  Scenario: a dropped change can be replayed
    Given a tab whose retrieve() dropped an insert for a key in the graveyard, named in the record by its change hash
    When the app calls replay() with that change hash
    Then the change was re-sent with a fresh sunset and decided like any change, and the record names it

  @criterion-01a14be5-057d-7163-9138-9085df6c897d @module-front-ends
  Scenario: @smugglr/web gives the CLI's record
    Given a tab opened with @smugglr/web naming an endpoint that has rows the tab does not
    When the tab syncs
    Then its run record uses the same field names and status words as the record of smugglr sync --json

  @criterion-01a14bed-6a79-7543-91fd-004394124faf @module-front-ends
  Scenario: @smugglr/web takes an ignore list
    Given a tab opened with @smugglr/web with ignore holding cache_*, naming an endpoint whose cache_pages has rows the tab lacks
    When the app calls sync()
    Then the tab's cache_pages has not changed

  @criterion-01a164ab-3807-74a1-a2bb-fa20725c9077 @module-front-ends
  Scenario: an app installs and imports the package
    Given an app with @smugglr/web installed from the packed package, and a stand-in endpoint holding rows
    When the app imports @smugglr/web and calls open() naming that endpoint
    Then the tab's database holds every row the endpoint holds
