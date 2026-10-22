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

  @criterion-01a1650a-6989-7173-870f-38f29c1875cd @module-front-ends
  Scenario Outline: a refused write is put back and reported
    Given a store made with <bridge> bound to todos, loaded, whose table refuses a row with no title
    When the app sets a key in the store to a value with no title
    Then the store holds what the table holds for that key, and the bridge's error listener received the refusal

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1650a-6989-7173-870f-390fb398fcda @module-front-ends
  Scenario Outline: a change made before the load is kept
    Given a tab whose database holds rows in todos, and a store made with <bridge> bound to todos whose load has not finished
    When the app sets a new key in the store before the load finishes
    Then once the load finishes, the store and the tab's database both hold that key

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a1650a-6989-7173-870f-3919c06cc167 @module-front-ends
  Scenario Outline: a reload does not overwrite a newer change
    Given a store made with <bridge> bound to todos, loaded, holding two rows
    When a sync changes one row and the app changes the other key in the store before the reload finishes
    Then the store holds the synced row and the app's change, and the tab's database holds both

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |

  @criterion-01a16511-f761-76b1-abd4-b50fc87d0d89 @module-front-ends
  Scenario Outline: a reload does not overwrite a change to the same key
    Given a store made with <bridge> bound to todos, loaded, holding a row
    When a sync changes that row and the app changes the same key in the store before the reload finishes
    Then the store and the tab's database both hold the app's change

    Examples:
      | bridge              |
      | @smugglr/nanostores |
      | @smugglr/zustand    |
