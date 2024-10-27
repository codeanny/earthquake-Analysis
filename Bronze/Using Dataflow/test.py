import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions  # Added StandardOptions
import requests
import logging
import datetime
import json

# Define your GCS bucket and API URL
BUCKET_NAME = "earthquake_data_anny"
API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

def fetch_data(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            logging.info(f"Data successfully retrieved from {url}")
            return response.json()
        else:
            logging.error(f"Failed to retrieve content from {url}. Status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error occurred while retrieving data from {url}: {e}")
        return None

def run_pipeline():
    # Set Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

    # Dataflow pipeline options
    options = PipelineOptions()

    # Specify Google Cloud options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'bwt-lear'
    google_cloud_options.job_name = "historical-data-load-dataflow"
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = f"gs://{BUCKET_NAME}/staging/"
    google_cloud_options.temp_location = f"gs://{BUCKET_NAME}/temp/"

    # Set the runner here
    options.view_as(StandardOptions).runner = 'DirectRunner'  # Use DataflowRunner for cloud execution

    # Fetch data from the API
    content = fetch_data(API_URL)

    # Check if content was retrieved successfully
    if content is not None:
        date_str = datetime.datetime.now().strftime('%Y%m%d')
        file_path = f'gs://{BUCKET_NAME}/Landing/{date_str}/historical_data'

        # Convert content to JSON string format for Apache Beam
        content_as_string = json.dumps(content)

        # Create and run the pipeline
        with beam.Pipeline(options=options) as p:
            (
                p
                | 'Create PCollection' >> beam.Create([content_as_string])  # Create a PCollection from the content
                | 'Write to GCS' >> beam.io.WriteToText(
                    file_path,
                    file_name_suffix='.json',
                    shard_name_template='',
                    num_shards=1
                )
            )
    else:
        logging.error("No content to upload to GCS.")

if __name__ == '__main__':
    run_pipeline()
