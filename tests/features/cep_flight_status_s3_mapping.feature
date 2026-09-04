Feature: CEP Flight Status — getFlightFlattened S3 event mapping (ACNP BAT)
  As a QA automation tester
  I want to verify flight events landed in S3 include every flat (dotted) field documented under
  data.customData.getFlightFlattened for type com.aircanada.airport_mobility.flight_status_change

  # Bucket: acnp-flight-event-capture-bat-cac1  (profile: CEP_ACNP_BAT)
  # Layout: s3://<bucket>/flight-events/<YYYY-MM-DD>/<Carrier><flight>-<date>-<airport>/AC-*.json
  # Payload may be full CloudEvent (data.*) or bare body (customData.* at top level).
  #
  # CEP_AWS_PROFILE  (default: CEP_ACNP_BAT)
  # FLIGHT_EVENTS_DATE (optional YYYY-MM-DD; default: today UTC)

  Scenario: Latest flight event JSON under today’s flight-events date prefix matches documented getFlightFlattened keys
    Given the CEP flight events S3 bucket "acnp-flight-event-capture-bat-cac1" in region "ca-central-1"
    And the flight events date folder is today UTC
    When I fetch the latest CEP flight event JSON under the flight-events date folder
    Then all documented getFlightFlattened keys should be present in the payload

  Scenario: Sample flight event JSON matches documented getFlightFlattened keys
    Given the CEP flight events S3 bucket "acnp-flight-event-capture-bat-cac1" in region "ca-central-1"
    And the CEP flight event S3 object key "flight-events/2026-04-16/AC610-2026-04-17-YYZ/AC-610-2026-04-17-YYZ_e77d8e0e-a2f9-4966-80e5-7f56c862b1ab.json"
    When I fetch the CEP flight event JSON from S3
    Then all documented getFlightFlattened keys should be present in the payload
