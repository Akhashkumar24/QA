Feature: S3 derived-event store output

  Scenario: JSON output exists under every configured prefix
    Given the use case
    When I list objects under each configured prefix
    Then every configured prefix holds the minimum number of JSON files

  Scenario: A current-date derived event matches the schema
    Given the use case
    When I fetch a random current-date derived event
    Then the derived event matches the use-case schema

  Scenario: Derived-event filenames follow the naming convention
    Given the use case
    When I fetch a random current-date derived event
    Then the derived-event filename matches the configured pattern
