# Script Name: config_module.py
# Script Description:
# This script contains configuration variables used throughout the project.
# Des: I have used the config_module.py file to manage constants like the service account path, API URL, and GCS bucket details.

# Update Date: 21-10-2024
#################################################

# config_module.py

# Google Cloud Project Configuration
GCP_PROJECT_ID = 'bwt-lear'
REGION = 'us-central1'
SERVICE_ACCOUNT = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = "earthquake_data_anny"
BRONZE_PATH = "/landing_layer/20241021/data.json"
SILVER_PATH = "Silver/20241021/flattened_data"
TEMP_LOCATION = 'gs://earthquake_data_anny/temp'  # Temporary location for Dataflow jobs

# BigQuery Configuration
BIGQUERY_TABLE = "earthquake_db.earthquake_data"

# Earthquake Data API URL
EARTHQUAKE_DATA_URL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson'
