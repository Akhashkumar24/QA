Feature: Lambda deployment configuration

  Scenario: Lambda is deployed as configured
    Given the use case
    When I retrieve the use-case Lambda configuration
    Then the Lambda runtime matches configuration
    And every configured Lambda environment variable matches
    And the Lambda carries the configured resource tags
    And the Lambda has the configured SQS event source enabled
    And the Lambda log retention matches configuration
