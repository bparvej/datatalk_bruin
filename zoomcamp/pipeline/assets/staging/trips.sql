/* @bruin

name: staging.trips
type: duckdb.sql
depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: create+replace

columns:
  - name: trip_id
    type: string
    description: "Unique identifier for each trip"
    primary_key: true
  - name: vendor_id
    type: integer
    description: "TLC vendor identifier"
  - name: pickup_datetime
    type: timestamp
    description: "Trip pickup timestamp"
  - name: dropoff_datetime
    type: timestamp
    description: "Trip dropoff timestamp"
  - name: passenger_count
    type: integer
    description: "Number of passengers"
  - name: trip_distance
    type: double
    description: "Trip distance in miles"
  - name: rate_code_id
    type: integer
    description: "Rate code identifier"
  - name: store_and_fwd_flag
    type: string
    description: "Store and forward flag"
  - name: pu_location_id
    type: integer
    description: "Pickup location ID"
  - name: do_location_id
    type: integer
    description: "Dropoff location ID"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: payment_type_name
    type: string
    description: "Payment type description"
  - name: fare_amount
    type: double
    description: "Fare amount"
  - name: extra
    type: double
    description: "Extra charges"
  - name: mta_tax
    type: double
    description: "MTA tax"
  - name: tip_amount
    type: double
    description: "Tip amount"
  - name: tolls_amount
    type: double
    description: "Tolls amount"
  - name: improvement_surcharge
    type: double
    description: "Improvement surcharge"
  - name: total_amount
    type: double
    description: "Total amount"
  - name: congestion_surcharge
    type: double
    description: "Congestion surcharge"
  - name: airport_fee
    type: double
    description: "Airport fee"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow, green, etc.)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"
  - name: source_file
    type: string
    description: "Source file name"

custom_checks:
  - name: row_count_check
    description: "Check that staging has reasonable row count compared to ingestion"
    query: |
      SELECT CASE WHEN COUNT(*) > 1000 THEN 0 ELSE 1 END as check_result
      FROM staging.trips
      WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
        AND tpep_pickup_datetime < '{{ end_datetime }}'
    value: 0


  - name: valid_payment_types
    description: "Ensure all payment types have valid names"
    query: |
      SELECT COUNT(*)
      FROM staging.trips
      WHERE payment_type_name IS NULL
        AND tpep_pickup_datetime >= '{{ start_datetime }}'
        AND tpep_pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

/* Staging layer: Clean, deduplicate, and enrich raw taxi trip data */
SELECT
  t.*,
  pl.payment_type_name
FROM ingestion.trips t
LEFT JOIN ingestion.payment_lookup pl
  ON t.payment_type = pl.payment_type_id
WHERE t.tpep_pickup_datetime >= '{{ start_datetime }}'
  AND t.tpep_pickup_datetime < '{{ end_datetime }}'
