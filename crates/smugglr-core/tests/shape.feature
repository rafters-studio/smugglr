# Generated from the legion requirement. Do not hand-edit this file.
Feature: a shape known only by its stamp (FR-SMUGGLR-070)

  @criterion-01a14c16-7274-7c32-92e5-47d8df2d2716 @module-smugglr-core
  Scenario: a stamp mismatch names what it can
    Given a device and a blob whose shape stamp for notes differs from the device's
    When the agent runs smugglr retrieve of that blob on the device with --json
    Then the record names notes, the device's columns of notes and both stamps
