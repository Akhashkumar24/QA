Feature: Validate ADM Events — Lambda Ingestion and S3 Output
  As a QA engineer,
  I want to verify that the ADM Lambda ingests events correctly
  and that the S3 derived event output has the expected schema and data types

  # ===========================================================================
  # Lambda Invocation — AC-Digital-BAT account
  # ===========================================================================

  Scenario: Invoke ADM Lambda with test event and validate success
    Given the ADM Lambda function "ac-odh-baggage-carousel-event-ingestion-batca1-parser" in region "ca-central-1"
    When I invoke the ADM Lambda with the ADM test event
    Then the ADM Lambda response status code should be 200
    And the ADM Lambda response should be printed

  # ---------------------------------------------------------------------------
  # CloudWatch Log Validation — verify successful ingestion
  # ---------------------------------------------------------------------------

  Scenario: CloudWatch logs confirm successful ADM Lambda ingestion
    Given the ADM Lambda function "ac-odh-baggage-carousel-event-ingestion-batca1-parser" in region "ca-central-1"
    When I invoke the ADM Lambda with the ADM test event
    And I search CloudWatch logs for the Lambda request ID
    Then the CloudWatch logs should contain ingestion success
    And the CloudWatch log details should be printed
    And the Lambda log tail should be printed

  # ---------------------------------------------------------------------------
  # Failed Ingestion — S3 events should NOT be stored
  # ---------------------------------------------------------------------------

  Scenario: Failed ingestion does not produce S3 events
    Given the ADM Lambda function "ac-odh-baggage-carousel-event-ingestion-batca1-parser" in region "ca-central-1"
    When I invoke the ADM Lambda with an invalid empty event
    And I search CloudWatch logs for the Lambda request ID
    Then the CloudWatch logs should indicate a processing error
    And no new S3 files should be stored for the invalid event
    And the CloudWatch log details should be printed

  # ===========================================================================
  # S3 Output Validation — AC-DATA-ODH-UAT account
  # ===========================================================================

  Scenario: Files exist under processed/CDM-FLIGHT-ODH-UAT prefix
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I list ADM JSON files under the prefix
    Then at least 1 ADM JSON file should exist under the prefix

  Scenario: Random current-date ADM event is valid JSON
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date ADM JSON file from the prefix
    Then the ADM file should exist and be readable
    And the ADM file content should be valid JSON
    And the full ADM event content should be printed

  # ---------------------------------------------------------------------------
  # eventType validation
  # ---------------------------------------------------------------------------

  Scenario: ADM event has eventType equal to ADMInfo
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date ADM JSON file from the prefix
    Then the ADM event field "eventType" should equal "ADMInfo"

  # ---------------------------------------------------------------------------
  # Carousel validation
  # ---------------------------------------------------------------------------

  Scenario: ADM event has Carousel field as integer or null
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date ADM JSON file from the prefix
    Then the ADM event should have field "Carousel"
    And the ADM event field "Carousel" should be an integer or null

  # ---------------------------------------------------------------------------
  # Id format validation — expected: {Carrier}-{Number}-{YYYY-MM-DD}-{Airport}
  # ---------------------------------------------------------------------------

  Scenario: ADM event Id matches expected format
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date ADM JSON file from the prefix
    Then the ADM event "Id" should match pattern "Carrier-Number-Date-Airport"

  # ---------------------------------------------------------------------------
  # DepartureAirport structure validation
  # ---------------------------------------------------------------------------

  Scenario: ADM event DepartureAirport has required fields
    Given the ADM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the ADM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date ADM JSON file from the prefix
    Then the ADM event should have field "DepartureAirport" as a dict
    And ADM "DepartureAirport.IATACode" should be a valid 3-letter IATA code
    And ADM "DepartureAirport.Terminal" should be present in the event
    And ADM "DepartureAirport.Gate" should be present in the event
