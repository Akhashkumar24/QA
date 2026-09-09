Feature: Lambda ingestion and CloudWatch confirmation

  Scenario: Lambda ingests its test event successfully
    Given the use case
    When I invoke the use-case Lambda with its test event
    Then the Lambda invocation is successful
    And the configured Lambda response fields are valid
    And the Lambda response is attached to the report
    When I search CloudWatch logs for the Lambda request id
    Then CloudWatch logs confirm ingestion success
    And the CloudWatch log details are attached to the report
