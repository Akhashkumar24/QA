Feature: S3 retention policy

  Scenario: The bucket enforces the configured retention
    Given the use case
    Then the bucket has the configured lifecycle retention rule
