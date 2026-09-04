Feature: Validate FDM Events — Lambda Ingestion and S3 Output
  As a QA engineer,
  I want to verify that the FDM Lambda ingests events correctly
  and that the S3 derived event output has the expected schema, status, ID format,
  and origin/destination airport data

  # ===========================================================================
  # Lambda Invocation — AC-Digital-BAT account
  # ===========================================================================

  Scenario: Invoke FDM Lambda with test event and validate success
    Given the FDM Lambda function "fdm-domain-ingestion-batca1-fdm-opensearch-update-lambda" in region "ca-central-1"
    When I invoke the FDM Lambda with the FDM test event
    Then the FDM Lambda response status code should be 200
    And the FDM Lambda response should be printed

  # ---------------------------------------------------------------------------
  # CloudWatch Log Validation — verify successful ingestion
  # ---------------------------------------------------------------------------

  Scenario: CloudWatch logs confirm successful FDM Lambda ingestion
    Given the FDM Lambda function "fdm-domain-ingestion-batca1-fdm-opensearch-update-lambda" in region "ca-central-1"
    When I invoke the FDM Lambda with the FDM test event
    And I search FDM CloudWatch logs for the Lambda request ID
    Then the FDM CloudWatch logs should contain ingestion success
    And the FDM CloudWatch log details should be printed
    And the FDM Lambda log tail should be printed

  # ---------------------------------------------------------------------------
  # Failed Ingestion — S3 events should NOT be stored
  # ---------------------------------------------------------------------------

  Scenario: Failed FDM ingestion does not produce S3 events
    Given the FDM Lambda function "fdm-domain-ingestion-batca1-fdm-opensearch-update-lambda" in region "ca-central-1"
    When I invoke the FDM Lambda with an invalid empty event
    And I search FDM CloudWatch logs for the Lambda request ID
    Then the FDM CloudWatch logs should indicate a processing error
    And no new FDM S3 files should be stored for the invalid event
    And the FDM CloudWatch log details should be printed

  # ===========================================================================
  # S3 Output Validation — AC-DATA-ODH-UAT account
  # ===========================================================================

  Scenario: Files exist under CDM/processed/CDM-FLIGHT-ODH-UAT prefix
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I list FDM JSON files under the prefix
    Then at least 1 FDM JSON file should exist under the prefix

  Scenario: Random current-date FDM event is valid JSON
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM file should exist and be readable
    And the FDM file content should be valid JSON
    And the full FDM event content should be printed

  # ---------------------------------------------------------------------------
  # eventType validation
  # ---------------------------------------------------------------------------

  Scenario: FDM event has eventType equal to FDMInfo
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM event field "eventType" should equal "FDMInfo"

  # ---------------------------------------------------------------------------
  # Id format validation — expected: {Carrier}-{Number}-{YYYY-MM-DD}-{Airport}
  # ---------------------------------------------------------------------------

  Scenario: FDM event Id matches expected format
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM event "Id" should match pattern "Carrier-Number-Date-Airport"

  # ---------------------------------------------------------------------------
  # Flight status validation
  # ---------------------------------------------------------------------------

  Scenario: FDM event has required status fields
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM event should have field "FlightStateCode"
    And the FDM event should have field "DepartureStatus" as a dict
    And FDM "DepartureStatus.Code" should be present in the event
    And the FDM event should have field "ArrivalStatus" as a dict
    And FDM "ArrivalStatus.Code" should be present in the event
    And the FDM event should have field "OverallStatus" as a dict
    And FDM "OverallStatus.Code" should be present in the event

  # ---------------------------------------------------------------------------
  # Origin (DepartureAirport) validation
  # ---------------------------------------------------------------------------

  Scenario: FDM event DepartureAirport has required fields
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM event should have field "DepartureAirport" as a dict
    And FDM "DepartureAirport.IATACode" should be a valid 3-letter IATA code
    And FDM "DepartureAirport.Gate" should be present in the event

  # ---------------------------------------------------------------------------
  # Destination (ArrivalAirport) validation
  # ---------------------------------------------------------------------------

  Scenario: FDM event ArrivalAirport has required fields
    Given the FDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the FDM S3 key prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    When I fetch a random current-date FDM JSON file from the prefix
    Then the FDM event should have field "ArrivalAirport" as a dict
    And FDM "ArrivalAirport.IATACode" should be a valid 3-letter IATA code
    And FDM "ArrivalAirport.Gate" should be present in the event
