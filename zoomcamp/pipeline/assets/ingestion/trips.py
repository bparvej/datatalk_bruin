"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  strategy: append


@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
from io import BytesIO


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Get taxi types from pipeline variables
    bruin_vars = json.loads(os.environ.get('BRUIN_VARS', '{}'))
    taxi_types = bruin_vars.get('taxi_types', ['yellow'])
    
    # Get date range from environment variables
    start_date_str = os.environ.get('BRUIN_START_DATE')
    end_date_str = os.environ.get('BRUIN_END_DATE')
    
    if not start_date_str or not end_date_str:
        raise ValueError("BRUIN_START_DATE and BRUIN_END_DATE environment variables are required")
    
    # Handle both date-only (YYYY-MM-DD) and datetime (ISO) formats
    try:
        # Try parsing as date first
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    except ValueError:
        # Try parsing as datetime
        start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00')).date()
        end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00')).date()
    
    # Convert back to datetime for month calculations
    start_date = datetime.combine(start_date, datetime.min.time())
    end_date = datetime.combine(end_date, datetime.min.time())
    
    # Validate date range - NYC taxi data is only available up to November 2025
    max_available_date = datetime(2025, 11, 30)
    if start_date > max_available_date:
        print(f"Warning: No taxi data available after November 2025. Requested start date: {start_date.date()}")
        return pd.DataFrame()  # Return empty DataFrame
    
    # Generate list of months to fetch (inclusive of start, exclusive of end)
    months = []
    current = start_date.replace(day=1)  # Start from beginning of start month
    while current < end_date:
        months.append(current)
        current += relativedelta(months=1)
    
    # Limit to available data
    months = [m for m in months if m <= max_available_date]
    
    # Base URL for TLC trip data
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    
    all_dataframes = []
    
    for taxi_type in taxi_types:
        for month in months:
            # Format: <taxi_type>_tripdata_<year>-<month>.parquet
            year_month = month.strftime('%Y-%m')
            filename = f"{taxi_type}_tripdata_{year_month}.parquet"
            url = base_url + filename
            
            try:
                # Download the parquet file
                print(f"Downloading {filename}...")
                response = requests.get(url, timeout=60)  # Add timeout
                response.raise_for_status()
                
                # Read parquet data into DataFrame
                df = pd.read_parquet(BytesIO(response.content))
                
                # Add metadata columns
                df['taxi_type'] = taxi_type
                df['extracted_at'] = datetime.now()
                df['source_file'] = filename
                
                all_dataframes.append(df)
                print(f"Successfully loaded {filename} with {len(df)} rows")
                
            except requests.exceptions.Timeout:
                print(f"Warning: Timeout downloading {filename}")
                continue
            except requests.exceptions.RequestException as e:
                print(f"Warning: Failed to download {filename}: {e}")
                continue
            except Exception as e:
                print(f"Warning: Failed to process {filename}: {e}")
                continue
    
    if not all_dataframes:
        print(f"Warning: No data was successfully downloaded for the specified date range ({start_date.date()} to {end_date.date()}) and taxi types {taxi_types}")
        print("Note: NYC taxi data is only available up to November 2025")
        return pd.DataFrame()  # Return empty DataFrame instead of raising error
    
    # Concatenate all DataFrames
    final_dataframe = pd.concat(all_dataframes, ignore_index=True)
    
    print(f"Successfully processed {len(final_dataframe)} total rows from {len(all_dataframes)} files")
    return final_dataframe


