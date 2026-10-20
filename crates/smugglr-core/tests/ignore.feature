# Generated from the legion requirement. Do not hand-edit this file.
Feature: ignored tables are never read, written, hashed or sent (FR-SMUGGLR-066)

  @criterion-01a14bed-6a79-7543-91fc-fff36b96d242 @module-smugglr-core
  Scenario: a configured table is ignored
    Given a config whose ignore list holds sessions_tmp, and two places whose sessions_tmp tables differ
    When the agent runs smugglr sync on them
    Then the target's sessions_tmp is unchanged

  @criterion-01a14bed-6a79-7543-91fd-000d94ff70bb @module-smugglr-core
  Scenario: a wildcard ignores every match
    Given a config whose ignore list holds cache_*, and two places whose cache_pages and cache_thumbs tables differ
    When the agent runs smugglr sync on them with --json
    Then the record's ignored list holds cache_pages and cache_thumbs

  @criterion-01a14bed-6a79-7543-91fd-0017554b5917 @module-smugglr-core
  Scenario: SQLite's own tables are ignored
    Given two places whose sqlite_sequence tables differ
    When the agent runs smugglr sync on them
    Then the target's sqlite_sequence is unchanged

  @criterion-01a14bed-6a79-7543-91fd-003421b1475b @module-smugglr-core
  Scenario: the record lists ignored tables once
    Given a config whose ignore list holds cache_* and sessions_tmp, and two places holding cache_pages, cache_thumbs and sessions_tmp
    When the agent runs smugglr sync on them with --json
    Then the record's ignored list names cache_pages, cache_thumbs and sessions_tmp once each
