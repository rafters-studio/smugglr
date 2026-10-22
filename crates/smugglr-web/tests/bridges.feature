# Generated from the legion requirement. Do not hand-edit this file.
Feature: @smugglr/nanostores and @smugglr/zustand make an app's store a view on the tab's database, one row per item (FR-SMUGGLR-082)

  @criterion-01a1631f-76d9-7a93-88b4-1d0294551813 @module-front-ends
  Scenario Outline: the store shows the tab's rows
    Given a tab whose database holds three rows in todos
    When the app makes a store with <bridge> bound to todos and its load finishes
    Then the store holds the three rows, each under its key

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d14e0872c5e @module-front-ends
  Scenario Outline: a key set in the store is a row
    Given a store made with <bridge> bound to todos, loaded
    When the app sets one key in the store
    Then the tab's database holds that row

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d2a6e7987be @module-front-ends
  Scenario Outline: a key removed from the store is a deleted row
    Given a store made with <bridge> bound to todos, loaded, holding a row
    When the app removes that key from the store
    Then the row is gone from the tab's database

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d3654790d3e @module-front-ends
  Scenario Outline: a sync reloads only the changed key
    Given a store made with <bridge> bound to todos, loaded, holding two rows
    When a sync changes one of the rows in the tab's database
    Then the store holds the changed row, and the other item is the same object as before

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d405dcb3a64 @module-front-ends
  Scenario Outline: no echo
    Given a store made with <bridge> bound to todos, loaded
    When the app sets one key in the store
    Then the store is notified once, for the app's own set

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d53d575db2f @module-front-ends
  Scenario Outline: nothing on the server
    Given a store made with <bridge> bound to todos, rendered on a server
    When the app reads the store
    Then the bridge has read and written nothing, and the store holds its initial state

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1631f-76d9-7a93-88b4-1d63211ecbe6 @module-front-ends
  Scenario: zustand says when it has loaded
    Given a tab whose database holds rows in todos
    When the app makes a store with @smugglr/zustand bound to todos
    Then hasHydrated() is false before the load finishes and true after, and an onFinishHydration listener was called once

  @criterion-01a1631f-76d9-7a93-88b4-1d77656d188c @module-front-ends
  Scenario: nanostores marks its load as a task
    Given a tab whose database holds rows in todos
    When the app mounts a store made with @smugglr/nanostores bound to todos and awaits allTasks()
    Then the store holds every row when allTasks() resolves
