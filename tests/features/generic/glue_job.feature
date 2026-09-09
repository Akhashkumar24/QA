Feature: Glue ETL job scheduling

  Scenario: Glue job exists and runs on the configured schedule
    Given the use case
    When I retrieve the use-case Glue job and its triggers
    Then the Glue job exists
    And a Glue trigger runs on the configured schedule in the configured state
