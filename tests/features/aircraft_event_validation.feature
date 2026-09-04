Feature: Validate Aircraft Events — Lambda Execution, S3 Output, and CloudWatch Logs
  As a software test analyst
  I want to invoke the Trax Aircraft Extractor Lambda, validate its execution,
  verify the S3 derived event output, and confirm CloudWatch logs are captured

  # ===========================================================================
  # Lambda Invocation & Response Validation
  # ===========================================================================

  Scenario: Invoke Lambda with test event and verify execution succeeded
    Given the Lambda function "ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I invoke the Lambda with a test event
    Then the Lambda execution status should be "Succeeded"
    And the Lambda response details should be printed

  Scenario: Lambda response contains a success message
    Given the Lambda function "ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I invoke the Lambda with a test event
    Then the Lambda response should contain a "message" field
    And the Lambda response message should indicate success

  Scenario: Lambda response reports total records processed
    Given the Lambda function "ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I invoke the Lambda with a test event
    Then the Lambda response should contain a "totalRecords" field
    And the total records count should be greater than 0

  Scenario: Lambda response reports success count matching total records
    Given the Lambda function "ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I invoke the Lambda with a test event
    Then the Lambda response should contain a "successCount" field
    And the success count should equal the total records count

  # ===========================================================================
  # CloudWatch Log Group Connectivity
  # ===========================================================================

  Scenario: CloudWatch log group exists and is accessible
    Given the CloudWatch log group "/aws/lambda/ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    Then the log group should exist

  # ===========================================================================
  # CloudWatch Logs — Success Message Validation
  # ===========================================================================

  Scenario: Trax Aircraft Extractor completed successfully
    Given the CloudWatch log group "/aws/lambda/ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I fetch recent logs containing "Trax Aircraft Extractor completed successfully"
    Then at least 1 matching log event should be found
    And each matching log event should be printed

  Scenario: Print the latest log events from the most recent stream
    Given the CloudWatch log group "/aws/lambda/ac-odh-Trax-Aircraft-Extractor-uatca1" in region "ca-central-1"
    When I fetch the latest log events from the most recent stream
    Then at least 1 log event should be returned
    And each log event should be printed

  # ===========================================================================
  # S3 Output — File Existence & Readability
  # ===========================================================================

  Scenario: Latest JSON file exists in the S3 bucket
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the file should exist and be readable
    And the file content should be valid JSON

  # ===========================================================================
  # S3 Output — File Naming Convention
  # ===========================================================================

  Scenario: File name follows the expected UUID naming pattern
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    Then the filename should match UUID pattern "{uuid}.json"

  # ===========================================================================
  # S3 Output — JSON Structure Validation
  # ===========================================================================

  Scenario: JSON file is an array containing at least one record
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the JSON content should be a non-empty array

  # ===========================================================================
  # S3 Output — Required Fields in Aircraft Record (PascalCase)
  # ===========================================================================

  Scenario: Aircraft record contains all required fields
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then each record should contain the field "FleetIdentificationNumber"
    And each record should contain the field "SerialNumber"
    And each record should contain the field "Manufacturer"
    And each record should contain the field "LastAcRegistration"
    And each record should contain the field "EventType"

  Scenario: FleetIdentificationNumber field is a non-empty string
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the field "FleetIdentificationNumber" in each record should be a non-empty string

  Scenario: SerialNumber field is a non-empty string
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the field "SerialNumber" in each record should be a non-empty string

  Scenario: Manufacturer field is a string or null
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the field "Manufacturer" in each record should be a string or null

  Scenario: LastAcRegistration field is a string or null
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the field "LastAcRegistration" in each record should be a string or null

  Scenario: EventType field should be AircraftInfo
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the field "EventType" in each record should equal "AircraftInfo"

  # ===========================================================================
  # S3 Output — Read & Log Full Record Content
  # ===========================================================================

  Scenario: Read latest JSON file and log aircraft record details
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the latest aircraft JSON file in the bucket
    When I fetch the file from S3
    Then the file should exist and be readable
    And the file content should be valid JSON
    And the JSON content should be a non-empty array
    And each record should log all field values

  # ===========================================================================
  # S3 Output — Specific File Validation (Known Aircraft Record)
  # ===========================================================================

  Scenario: Validate aircraft-518 file contains expected data
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    And the key prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    And the specific file "019ca5e9-5529-700a-a1b8-b9f2b453c1e7.json" in the bucket
    When I fetch the file from S3
    Then the file should exist and be readable
    And the file content should be valid JSON
    And the field "FleetIdentificationNumber" in record 0 should be "518"
    And the field "SerialNumber" in record 0 should be "61223"
    And the field "Manufacturer" in record 0 should be "BOEING"
    And the field "LastAcRegistration" in record 0 should be "C-FSOI"
    And the field "EventType" in record 0 should be "AircraftInfo"

  # ===========================================================================
  # AWS Resource Validation — S3 Bucket Data Retention
  # ===========================================================================

  Scenario: S3 bucket has a data retention policy of 30 days
    Given the S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    When I fetch the lifecycle configuration for the bucket
    Then a lifecycle rule with expiration of 30 days should exist
    And the lifecycle rule should be enabled
    And the lifecycle rule details should be printed
