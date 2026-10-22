# Generated from the legion requirement. Do not hand-edit this file.
Feature: one engine, one row hash, native and in WASM (FR-SMUGGLR-058)

  @criterion-01a14be5-057d-7163-9138-905f19040133 @module-smugglr-core
  Scenario: one row, one hash, native and in WASM
    Given a row holding the integer 9007199254740993
    When the native engine and the WASM engine each hash the row
    Then the two hashes are identical

  @criterion-01a14be5-057d-7163-9138-9064dbf7fa68 @module-smugglr-core
  Scenario: NULL, empty text and an empty blob hash apart
    Given three rows alike except one column, holding NULL in the first, empty text in the second and an empty blob in the third
    When the engine hashes the three rows
    Then the three hashes all differ

  @criterion-01a15311-1191-7862-953b-cee711d85553 @module-smugglr-core
  Scenario: a known row hashes to its known value
    Given the row whose columns' quote() text is 1 and 'smugglr'
    When the engine hashes the row
    Then the hash is the 16-byte value recorded for that row

  @criterion-01a14be5-057d-7163-9138-909f4c4ba1a0 @module-smugglr-core
  Scenario: the browser build hashes like native
    Given a row holding 9007199254740993 and rows holding NULL, empty text and an empty blob
    When the browser's SQLite build and the native engine each hash them
    Then each row's two hashes are identical
