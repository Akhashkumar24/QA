Feature: Validate Glue Job and S3 Output for Flight Ground Time
  As a QA engineer,
  I want to verify the Ground Time Glue job exists with correct scheduling
  and that the S3 derived event output has the expected schema and data types

  # ===========================================================================
  # Glue Job Validation — AC-Digital-BAT account
  # ===========================================================================

  Scenario: Glue job exists in the account
    Given the Glue job "ac-odh-flight-leg-ground-times-mst-batca1-data-job" in region "ca-central-1"
    When I retrieve the Glue job configuration
    Then the Glue job should exist
    And the Glue job details should be printed

  Scenario: Glue job has a trigger scheduled to run every 30 minutes
    Given the Glue job "ac-odh-flight-leg-ground-times-mst-batca1-data-job" in region "ca-central-1"
    When I retrieve the triggers for the Glue job
    Then at least 1 trigger should be associated with the Glue job
    And the trigger schedule should be every 30 minutes
    And the trigger should be in "ACTIVATED" state

  # ===========================================================================
  # S3 Output Validation — ac-odh-derived-event-storage-uat-cac-1
  # ===========================================================================

  # ---------------------------------------------------------------------------
  # Files exist under processed CDM prefix
  # ---------------------------------------------------------------------------

  Scenario: Files exist under CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT prefix
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I list JSON files under the prefix
    Then at least 1 JSON file should exist under the prefix

  # ---------------------------------------------------------------------------
  # Latest file — existence and valid JSON
  # ---------------------------------------------------------------------------

  Scenario: Random current-date processed file is valid JSON
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then the file should exist and be readable
    And the file content should be valid JSON
    And the full event content should be printed

  # ---------------------------------------------------------------------------
  # Top-level schema — required fields and types
  # ---------------------------------------------------------------------------

  Scenario: Event contains all required top-level fields
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then the event should have field "eventType" of type "string"
    And the event should have field "Flight" of type "dict"
    And the event should have field "InboundFlight" of type "dict"
    And the event should have field "ScheduledGroundTime" of type "string"
    And the event should have field "MinimumGroundTime" of type "string"
    And the event should have field "MinimumDepartureGroundTime" of type "string"
    And the event should have field "EstimatedGroundTime" of type "string"
    And the event should have field "GoTime" of type "string"

  # ---------------------------------------------------------------------------
  # Flight object — nested field datatype validation
  # ---------------------------------------------------------------------------

  Scenario: Flight object contains all required fields with correct types
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then "Flight.OperatingCarrier.Code" should be a non-empty string
    And "Flight.OperatingNumber" should be a non-empty string
    And "Flight.DepartureAirport.IATACode" should be a valid 3-letter IATA code
    And "Flight.DepartureAirport.Country.Code" should be a valid 2-letter country code
    And "Flight.ArrivalAirport.IATACode" should be a valid 3-letter IATA code
    And "Flight.ArrivalAirport.Country.Code" should be a valid 2-letter country code
    And "Flight.OriginDepartureAt" should be a valid date in YYYY-MM-DD format
    And "Flight.Aircraft.FleetIdentificationNumber" should be a string
    And "Flight.Aircraft.EquipmentType.IATACode" should be a non-empty string

  # ---------------------------------------------------------------------------
  # InboundFlight object — nested field datatype validation
  # ---------------------------------------------------------------------------

  Scenario: InboundFlight object contains all required fields with correct types
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then "InboundFlight.OperatingCarrier.Code" should be a non-empty string
    And "InboundFlight.OperatingNumber" should be a non-empty string
    And "InboundFlight.DepartureAirport.IATACode" should be a valid 3-letter IATA code
    And "InboundFlight.DepartureAirport.Country.Code" should be a valid 2-letter country code
    And "InboundFlight.ArrivalAirport.IATACode" should be a valid 3-letter IATA code
    And "InboundFlight.ArrivalAirport.Country.Code" should be a valid 2-letter country code
    And "InboundFlight.OriginDepartureAt" should be a valid date in YYYY-MM-DD format
    And "InboundFlight.Aircraft.FleetIdentificationNumber" should be a string
    And "InboundFlight.Aircraft.EquipmentType.IATACode" should be a non-empty string

  # ---------------------------------------------------------------------------
  # Ground time fields — HH:MM:SS format validation
  # ---------------------------------------------------------------------------

  Scenario: Ground time fields are in HH:MM:SS format
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then "ScheduledGroundTime" should match HH:MM:SS time format
    And "MinimumGroundTime" should match HH:MM:SS time format
    And "MinimumDepartureGroundTime" should match HH:MM:SS time format
    And "EstimatedGroundTime" should match HH:MM:SS time format

  # ---------------------------------------------------------------------------
  # GoTime — ISO 8601 UTC datetime format validation
  # ---------------------------------------------------------------------------

  Scenario: GoTime is a valid ISO 8601 UTC datetime
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then "GoTime" should be a valid ISO 8601 UTC datetime

  # ---------------------------------------------------------------------------
  # Data consistency — InboundFlight arrival = Flight departure
  # ---------------------------------------------------------------------------

  Scenario: InboundFlight arrival airport matches Flight departure airport
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then InboundFlight ArrivalAirport should match Flight DepartureAirport

  Scenario: Flight and InboundFlight share the same aircraft
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the S3 key prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    When I fetch a random current-date JSON file with complete data from the prefix
    Then Flight and InboundFlight should have the same FleetIdentificationNumber

  # ===========================================================================
  # MySQL Database Validation — Digital-ODS BATCA1 via SSH Tunnel
  # ===========================================================================

  Scenario: Connect to Digital-ODS BATCA1 database via SSH tunnel
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    Then the database connection should be established
    And the connection details should be printed

  Scenario: FlightLegGroundTime table exists in the database
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SHOW TABLES LIKE 'FlightLegGroundTime'"
    Then the query result should have at least 1 row
    And the query result should be printed

  Scenario: FlightLegGroundTime table schema matches expected columns
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "DESCRIBE FlightLegGroundTime"
    Then the query result should have at least 1 row
    And the query result should be printed

  Scenario: Fetch sample rows from FlightLegGroundTime table
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SELECT * FROM FlightLegGroundTime ORDER BY RecordModifiedAt DESC LIMIT 10"
    Then the query result should have at least 1 row
    And the query result should be printed

  Scenario: Fetch row count from FlightLegGroundTime table
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SELECT COUNT(*) AS TotalRows FROM FlightLegGroundTime"
    Then the query result should have at least 1 row
    And the query result should be printed

  Scenario: Verify new schema columns exist in FlightLegGroundTime
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'FlightScheduleDataStore' AND TABLE_NAME = 'FlightLegGroundTime' ORDER BY ORDINAL_POSITION"
    Then the query result should have at least 1 row
    And the result should contain column "FlightLegDepartureCountryCode"
    And the result should contain column "FlightLegArrivalCountryCode"
    And the result should contain column "EstimatedGroundTime"
    And the result should contain column "GoTime"
    And the query result should be printed

  Scenario: Fetch FlightLegGroundTime data for FlightLegNumber 9883
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SELECT * FROM FlightScheduleDataStore.FlightLegGroundTime WHERE FlightLegNumber = '9883'"
    Then the query result should have at least 1 row
    And the query result should be printed

  Scenario: DB data for FlightLegNumber 9883 matches S3 event values
    Given the Digital-ODS BATCA1 database connection details
    When I connect to the database via SSH tunnel
    And I execute the query "SELECT * FROM FlightScheduleDataStore.FlightLegGroundTime WHERE FlightLegNumber = '9883'"
    Then the query result should have at least 1 row
    And DB column "FlightLegCarrierCode" should equal "AC"
    And DB column "FlightLegNumber" should equal "9883"
    And DB column "FlightLegDepartureAirportCode" should equal "YHZ"
    And DB column "FlightLegDepartureCountryCode" should equal "CA"
    And DB column "ScheduledGroundTime" in minutes should equal time "24:49:00"
    And DB column "MinimumGroundTime" in minutes should equal time "00:50:00"
    And DB column "MinimumDepartureGroundTime" in minutes should equal time "00:45:00"
    And DB column "EstimatedGroundTime" in minutes should equal time "24:49:00"
    And DB column "GoTime" in minutes should equal UTC time "2026-03-21T15:55:00.000Z"
    And the query result should be printed

