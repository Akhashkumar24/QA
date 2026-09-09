Feature: S3 object field mapping

  Scenario: Configured objects contain every documented field
    Given the use case
    When I fetch each configured derived-event object
    Then every configured derived-event object matches its schema

  Scenario: The latest event under today's date folder is fully mapped
    Given the use case
    When I fetch the latest derived event under the use-case date folder
    Then the derived event matches the use-case schema
