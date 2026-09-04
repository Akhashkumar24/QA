Feature: CDM — [FlightStatus] Canonical data model derived event publisher (S3 → Kafka path)
  As a QA automation engineer
  I want to validate the CDM derived-event publisher Lambda, S3 derived-store paths,
  CloudWatch logging, and operational tagging so that events flow from the derived
  event store toward CEP (Kafka) with correct platform settings

  # Kafka topic delivery and exponential retry behaviour are best validated with
  # integration tests or observability; this suite asserts AWS resources and S3
  # data paths that implement the story in UAT.

  # -----------------------------------------------------------------------------
  # Configuration — optional overrides
  # CDM_DERIVED_EVENT_PUBLISHER_LAMBDA  (default: ac-odh-CDM-Event-Processor-uatca1)
  # CDM_AWS_PROFILE                     (default: ODH_UAT — same account as UAT bucket)
  # -----------------------------------------------------------------------------

  # ===========================================================================
  # Lambda — Node.js runtime, tags (incl. Dynatrace), environment visibility
  # ===========================================================================

  Scenario: CDM publisher Lambda exists and uses a current Node.js runtime
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    When I retrieve the CDM Lambda function configuration
    Then the CDM Lambda should exist
    And the CDM Lambda runtime should be Node.js 24.x
    And the CDM Lambda configuration details should be printed

  Scenario: CDM publisher Lambda carries Dynatrace-related resource tags
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    When I retrieve the CDM Lambda function configuration
    And I list tags for the CDM Lambda function
    Then the CDM Lambda should have Dynatrace-related tags
    And the CDM Lambda tags should be printed

  Scenario: CDM Lambda environment variables are visible for log-level verification
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    When I retrieve the CDM Lambda function configuration
    Then the CDM Lambda environment variables should be printed

  Scenario: CDM Lambda environment variables match UAT deployment expectations
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    When I retrieve the CDM Lambda function configuration
    Then the CDM Lambda environment variable "ARCHIVE_PREFIX" should equal "CDM/processed/"
    And the CDM Lambda environment variable "DLQ_PREFIX" should equal "CDM/kafka-failures/"
    And the CDM Lambda environment variable "EVENT_BUCKET" should equal "ac-odh-derived-event-storage-uat-cac-1"
    And the CDM Lambda environment variable "KAFKA_TOPIC_AIRCRAFT" should equal "CDM-AIRCRAFT-ODH-UAT"
    And the CDM Lambda environment variable "KAFKA_TOPIC_FLIGHT" should equal "CDM-FLIGHT-ODH-UAT"
    And the CDM Lambda environment variable "KAFKA_TOPIC_FLIGHTGROUNDTIME" should equal "CDM-FLIGHTGROUNDTIME-ODH-UAT"
    And the CDM Lambda environment variable "ON_KAFKA_FAILURE" should equal "DLQ"
    And the CDM Lambda environment variable "ON_SUCCESS" should equal "MOVE"

  # ===========================================================================
  # Lambda — SQS event source and dead-letter queue (failed messages)
  # ===========================================================================

  Scenario: CDM Lambda has an enabled SQS event source mapping
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    When I retrieve event source mappings for the CDM Lambda
    Then the CDM Lambda should have an enabled SQS event source for queue "ac-odh-cdm-event-publisher-uatca1-queue"

  Scenario: CDM primary SQS queue routes failed messages to the DLQ
    Given the CDM SQS queue "ac-odh-cdm-event-publisher-uatca1-queue" in region "ca-central-1"
    When I retrieve RedrivePolicy for the CDM SQS queue
    Then the CDM queue dead letter target should be "ac-odh-cdm-event-publisher-uatca1-dlq"

  # Sends a payload that should fail Lambda processing; after RedrivePolicy retries
  # (e.g. maxReceiveCount) the message lands in the DLQ. Override wait with CDM_DLQ_WAIT_SECONDS.
  # IAM: sqs:SendMessage on the primary queue; sqs:ReceiveMessage + sqs:DeleteMessage on the DLQ.
  # If SendMessage is denied, the scenario is skipped (RedrivePolicy scenario still validates config).
  Scenario: A failing test message is moved to the DLQ after Lambda retries
    Given the CDM SQS queue "ac-odh-cdm-event-publisher-uatca1-queue" in region "ca-central-1"
    And the CDM SQS DLQ "ac-odh-cdm-event-publisher-uatca1-dlq" in region "ca-central-1"
    When I send a CDM QA test message that should fail Lambda processing
    And I wait for that test message to appear in the CDM DLQ
    Then the CDM DLQ message body should contain the test correlation id
    And I delete the CDM test message from the DLQ

  # ===========================================================================
  # CloudWatch — log group, retention (non-prod UAT: typically 14–15 days)
  # ===========================================================================

  Scenario: CloudWatch log group exists for the CDM publisher Lambda
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    And the CloudWatch log group for the CDM Lambda in region "ca-central-1"
    Then the CDM log group should exist

  Scenario: CloudWatch log retention matches non-prod policy for UAT
    Given the CDM derived event publisher Lambda in region "ca-central-1"
    And the CloudWatch log group for the CDM Lambda in region "ca-central-1"
    When I describe the CDM CloudWatch log group
    Then the CDM log group retention should be between 14 and 15 days inclusive

  # ===========================================================================
  # S3 — derived event store: source folders (ingest) and processed prefixes (pipeline)
  # ===========================================================================

  Scenario: Derived-event source prefixes are accessible in the bucket
    Given the CDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    When I list objects under the CDM S3 prefix "CDM-FLIGHT-ODH/"
    Then the CDM S3 list operation should succeed
    When I list objects under the CDM S3 prefix "CDM-AIRCRAFT-ODH/"
    Then the CDM S3 list operation should succeed
    When I list objects under the CDM S3 prefix "CDM-FLIGHTGROUNDTIME-ODH/"
    Then the CDM S3 list operation should succeed

  Scenario: Processed CDM paths contain at least one JSON artifact each
    Given the CDM S3 bucket "ac-odh-derived-event-storage-uat-cac-1" in region "ca-central-1"
    When I list JSON files under the CDM S3 prefix "CDM/processed/CDM-FLIGHT-ODH-UAT/"
    Then at least 1 JSON file should exist under the CDM prefix
    When I list JSON files under the CDM S3 prefix "CDM/processed/CDM-AIRCRAFT-ODH-UAT/"
    Then at least 1 JSON file should exist under the CDM prefix
    When I list JSON files under the CDM S3 prefix "CDM/processed/CDM-FLIGHTGROUNDTIME-ODH-UAT/"
    Then at least 1 JSON file should exist under the CDM prefix
