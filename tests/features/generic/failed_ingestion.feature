Feature: Failed ingestion produces no derived event

  Scenario: The pipeline rejects an invalid event without storing output
    Given the use case
    When I invoke the use-case Lambda with its invalid test event
    And I search CloudWatch logs for the Lambda request id
    Then CloudWatch logs indicate a processing error
    And no derived event is stored after the invalid invocation
    And the CloudWatch log details are attached to the report
