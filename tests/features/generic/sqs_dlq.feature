Feature: SQS dead-letter routing

  Scenario: The primary queue is wired to its DLQ
    Given the use case
    Then the primary queue redrive policy targets the configured DLQ

  Scenario: A failing message lands in the DLQ
    Given the use case
    When I send a failing test message and wait for it in the DLQ
    Then the DLQ message carries the test correlation id
    And I remove the test message from the DLQ
