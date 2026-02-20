/* @bruin

name: reports.trips

type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: create+replace

columns:
  - name: pickup_date
    type: date
    description: "Date of trip pickup"
    primary_key: true
  - name: payment_type_name
    type: string
    description: "Payment type"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Type of taxi"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Number of trips"
    checks:
      - name: non_negative
  - name: total_revenue
    type: double
    description: "Total fare revenue"
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: double
    description: "Average fare amount per trip"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: "Average trip distance in miles"
    checks:
      - name: non_negative
  - name: avg_tip_amount
    type: double
    description: "Average tip amount per trip"
    checks:
      - name: non_negative
  - name: total_passengers
    type: bigint
    description: "Total number of passengers"
    checks:
      - name: non_negative

@bruin */

-- Daily trip summary report aggregated by payment type and taxi type
SELECT
  CAST(tpep_pickup_datetime AS DATE) as pickup_date,
  COALESCE(payment_type_name, 'Unknown') as payment_type_name,
  taxi_type,
  COUNT(*) as trip_count,
  SUM(total_amount) as total_revenue,
  AVG(fare_amount) as avg_fare_amount,
  AVG(trip_distance) as avg_trip_distance,
  AVG(tip_amount) as avg_tip_amount,
  SUM(passenger_count) as total_passengers
FROM staging.trips
WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(tpep_pickup_datetime AS DATE),
  COALESCE(payment_type_name, 'Unknown'),
  taxi_type
