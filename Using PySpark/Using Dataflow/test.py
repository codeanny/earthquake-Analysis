import os
import json
from datetime import datetime
import requests
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from google.cloud import storage

# GCP configurations
SERVICE_ACCOUNT_KEY = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"
PROJECT_ID = "bwt-lear"
GCS_BUCKET = "earthquake_data_anny"
DATASET_ID = "earthquake_dataset"
TABLE_ID = "earthquake_daily_data_dataflow"
TABLE_REF = f'{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}'
SOURCE_API_URL_DAILY = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

# File paths
c_date = datetime.now().strftime('%Y%m%d')
DATAFLOW_LANDING_LOCATION = f"dataflow_bronz/landing_daily/{c_date}/raw_{c_date}.json"
DATAFLOW_SILVER_LAYER_PATH = f"dataflow_silver/silver_daily/{c_date}/transformed_{c_date}"
DATAFLOW_GOLD_LAYER_PATH = f"dataflow_gold/{c_date}/transformed_{c_date}"

# GCP Pipeline options
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY
options = PipelineOptions(save_main_session=True, temp_location=f"gs://{GCS_BUCKET}/temp")
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = PROJECT_ID
google_cloud_options.job_name = f"dataflow-earthquake-{datetime.now().strftime('%y%m%d-%H%M%S')}"
google_cloud_options.region = 'us-central1'
google_cloud_options.staging_location = f"gs://{GCS_BUCKET}/staging/dataflow"
options.view_as(StandardOptions).runner = 'DirectRunner'


# Function to fetch data from API
def fetch_data_from_api(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


# Function to upload JSON data to GCS
def upload_data_to_gcs(bucket_name, destination_file_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_file_name)
    blob.upload_from_string(json.dumps(data), content_type='application/json')


# Class to fetch data and upload to GCS
class FetchAndUpload(beam.DoFn):
    def __init__(self, api_url, bucket_name, destination_file_name):
        self.api_url = api_url
        self.bucket_name = bucket_name
        self.destination_file_name = destination_file_name

    def process(self, element):
        data = fetch_data_from_api(self.api_url)
        upload_data_to_gcs(self.bucket_name, self.destination_file_name, data)
        yield f"gs://{self.bucket_name}/{self.destination_file_name}/raw_data_fetched.json"


# Class for data transformation
class FlattenPlusTransformations(beam.DoFn):
    def process(self, element):
        input_data = json.loads(element)
        features = input_data['features']
        for feature in features:
            properties = feature.get('properties', {})
            geometry = feature.get('geometry', {}).get('coordinates', [None, None, None])
            place = properties.get('place', "")
            area = place.split(" of ")[1] if " of " in place else None
            event_time = properties.get('time')
            last_update = properties.get('updated')

            result = {
                'magnitude': properties.get('mag', 0.0),
                'event_time': datetime.utcfromtimestamp(event_time / 1000).isoformat() if event_time else None,
                'last_update': datetime.utcfromtimestamp(last_update / 1000).isoformat() if last_update else None,
                'timezone_offset': properties.get('tz', 0),
                'info_url': properties.get('url', ""),
                'description': properties.get('detail', ""),
                'felt_reports': properties.get('felt', 0),
                'cdi_value': properties.get('cdi', 0.0),
                'mmi_value': properties.get('mmi', 0.0),
                'alert_status': properties.get('alert', ""),
                'event_status': properties.get('status', ""),
                'tsunami_warning': properties.get('tsunami', 0),
                'significance': properties.get('sig', 0),
                'network_code': properties.get('net', ""),
                'event_code': properties.get('code', ""),
                'event_ids': properties.get('ids', ""),
                'data_sources': properties.get('sources', ""),
                'event_types': properties.get('types', ""),
                'station_count': properties.get('nst', 0),
                'min_distance': properties.get('dmin', 0.0),
                'rms_value': properties.get('rms', 0.0),
                'gap_angle': properties.get('gap', 0.0),
                'magnitude_type': properties.get('magType', ""),
                'event_type': properties.get('type', ""),
                'location': {
                    'longitude': geometry[0] if geometry[0] else 0.0,
                    'latitude': geometry[1] if geometry[1] else 0.0,
                    'depth': geometry[2] if geometry[2] else 0.0
                },
                'area': area or ""
            }
            yield beam.pvalue.TaggedOutput('silver', result)


# BigQuery schema
schema = {
    'fields': [
        {'name': 'magnitude', 'type': 'FLOAT'},
        {'name': 'event_time', 'type': 'TIMESTAMP'},
        {'name': 'last_update', 'type': 'TIMESTAMP'},
        {'name': 'timezone_offset', 'type': 'INTEGER'},
        {'name': 'info_url', 'type': 'STRING'},
        {'name': 'description', 'type': 'STRING'},
        {'name': 'felt_reports', 'type': 'INTEGER'},
        {'name': 'cdi_value', 'type': 'FLOAT'},
        {'name': 'mmi_value', 'type': 'FLOAT'},
        {'name': 'alert_status', 'type': 'STRING'},
        {'name': 'event_status', 'type': 'STRING'},
        {'name': 'tsunami_warning', 'type': 'INTEGER'},
        {'name': 'significance', 'type': 'INTEGER'},
        {'name': 'network_code', 'type': 'STRING'},
        {'name': 'event_code', 'type': 'STRING'},
        {'name': 'event_ids', 'type': 'STRING'},
        {'name': 'data_sources', 'type': 'STRING'},
        {'name': 'event_types', 'type': 'STRING'},
        {'name': 'station_count', 'type': 'INTEGER'},
        {'name': 'min_distance', 'type': 'FLOAT'},
        {'name': 'rms_value', 'type': 'FLOAT'},
        {'name': 'gap_angle', 'type': 'FLOAT'},
        {'name': 'magnitude_type', 'type': 'STRING'},
        {'name': 'event_type', 'type': 'STRING'},
        {'name': 'location', 'type': 'RECORD', 'fields': [
            {'name': 'longitude', 'type': 'FLOAT'},
            {'name': 'latitude', 'type': 'FLOAT'},
            {'name': 'depth', 'type': 'FLOAT'}
        ]},
        {'name': 'area', 'type': 'STRING'}
    ]
}

# Pipeline
with beam.Pipeline(options=options) as pipeline:
    # Step 1: Fetch and upload data
    raw_data = (
            pipeline
            | "Create dummy" >> beam.Create([None])
            | "Fetch and Upload to GCS" >> beam.ParDo(FetchAndUpload(SOURCE_API_URL_DAILY, GCS_BUCKET, DATAFLOW_LANDING_LOCATION))
    )

    # Step 2: Transform data
    transformed_data = (
            raw_data
            | "Read JSON from GCS" >> beam.io.ReadFromText(f"gs://{GCS_BUCKET}/{DATAFLOW_LANDING_LOCATION}")
            | "Flatten and Transform" >> beam.ParDo(FlattenPlusTransformations()).with_outputs('silver')
    )

    # # Step 3: Write transformed data to BigQuery
    # (
    #         transformed_data['silver']
    #         | "Write to BigQuery" >> beam.io.WriteToBigQuery(
    #     TABLE_REF,
    #     schema=schema,
    #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    # )
    # )
