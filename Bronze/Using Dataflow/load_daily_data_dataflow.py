import json
from datetime import datetime
import re
from requests.exceptions import HTTPError
import logging
import os
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
import apache_beam as beam
from apache_beam import DoFn
from utility import upload_to_gcs, fetch_api_data, read_from_gcs, flatten_feature

# Define your GCS bucket and API URL
BUCKET_NAME = "earthquake_data_anny"
url = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'

# Define the FetchAPIData class
class FetchAPIData(DoFn):
    def process(self, element):
        try:
            geojson_data = fetch_api_data(element)  # Assuming 'element' is a URL
            yield geojson_data
        except HTTPError as e:
            logging.error(f"HTTP error occurred: {e}")
        except Exception as e:
            logging.error(f"An error occurred: {e}")

# Flatten the feature collection
class FlattenFeature(DoFn):
    def process(self, feature_collection):
        logging.info(f"Processing feature_collection of type: {type(feature_collection)}")
        if isinstance(feature_collection, str):
            feature_collection = json.loads(feature_collection)
        flattened_records = flatten_feature(feature_collection)
        for record in flattened_records:
            yield record

# Convert timestamps from epoch to human-readable format
class ConvertTimestamp(DoFn):
    def process(self, element):
        # Convert Apache Beam Row to a dictionary if needed
        if isinstance(element, beam.pvalue.Row):
            element = element._asdict()

        # Ensure the input is a dictionary
        if not isinstance(element, dict):
            logging.error("Expected a dictionary but received: %s", type(element))
            return

        # Convert 'time' from epoch to local timestamp if it's present
        if 'time' in element and element['time'] is not None:
            try:
                epoch_time = float(element['time']) / 1000
                element['time'] = datetime.fromtimestamp(epoch_time).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logging.error(f"Error converting 'time': {e}")

        # Convert 'updated' from epoch to local timestamp if it's present
        if 'updated' in element and element['updated'] is not None:
            try:
                epoch_updated = float(element['updated']) / 1000
                element['updated'] = datetime.fromtimestamp(epoch_updated).strftime('%Y-%m-%d %H:%M:%S')
            except Exception as e:
                logging.error(f"Error converting 'updated': {e}")

        # Optionally extract 'area' from 'place' field if it exists
        if 'place' in element and element['place']:
            try:
                pattern = r'of\s+(.*)'
                match = re.search(pattern, element['place'])
                if match:
                    element['area'] = match.group(1).strip()
            except Exception as e:
                logging.error(f"Error extracting 'area': {e}")

        # Yield the modified record
        yield element

# Add current insert date
class AddInsertDate(DoFn):
    def process(self, element):
        if isinstance(element, str):
            record = json.loads(element)
        else:
            record = element
        record['insert_date'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        yield record

if __name__ == "__main__":
    # Define Pipeline Options
    options = PipelineOptions()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\earthquake-ingestion\eathquake-73bed61ccae3.json'
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'bwt-lear'
    google_cloud_options.job_name = "Daily-data-load-dataflow"
    google_cloud_options.region = 'us-central1'
    google_cloud_options.staging_location = f"gs://{BUCKET_NAME}/staging/"
    google_cloud_options.temp_location = f"gs://{BUCKET_NAME}/temp/"
    options.view_as(StandardOptions).runner = 'DirectRunner'

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD
    #
    # # Step 1: Fetch GeoJSON data from the API
    # with beam.Pipeline(options=options) as p1:
    #     api_urls = [url]
    #     geojson_data = (
    #         p1
    #         | "Create API URLs" >> beam.Create(api_urls)
    #         | "Fetch API Data Step 1" >> beam.ParDo(FetchAPIData())
    #     )
    #     output_path_1 = f'gs://earthquake_data_anny/landing/{current_date}/dailydata.json'
    #     upload_result_1 = (
    #         geojson_data
    #         | 'Format as JSON Step 1' >> beam.Map(lambda x: json.dumps(x))
    #         | 'Write to GCS Step 1' >> beam.io.WriteToText(output_path_1, file_name_suffix='.json', num_shards=1)
    #     )
    #
    # # Step 2: Read from GCS and process data
    # input_path_2 = f'gs://earthquake_data_anny/landing/{current_date}/dailydata.*'
    # with beam.Pipeline(options=options) as p2:
    #     read_result = (
    #         p2
    #         | 'Read from GCS Step 2' >> beam.io.ReadFromText(input_path_2)
    #     )
    #     flattened_data = (
    #         read_result
    #         | "Flatten Data Step 2" >> beam.ParDo(FlattenFeature())
    #     )
    #     processed_data = flattened_data | 'Convert Timestamps Step 2' >> beam.ParDo(ConvertTimestamp())
    #
    #     # Optional step to add current date
    #     processed_data | 'Print Processed Data Step 2' >> beam.Map(print)
    #     output_path_2 = f'gs://earthquake_data_anny/silver/{current_date}/dailydata.json'
    #     upload_result_2 = (
    #         processed_data
    #         | 'Format as JSON Step 2' >> beam.Map(lambda x: json.dumps(x))
    #         | 'Write to GCS Step 2' >> beam.io.WriteToText(output_path_2, file_name_suffix='.json', num_shards=1)
    #     )

    # Step 3: Read from GCS and write to BigQuery
    input_path_3 = f'gs://earthquake_data_anny/silver/{current_date}/dailydata.*'
    with beam.Pipeline(options=options) as p3:
        read_result_3 = (
            p3
            | 'Read from GCS2' >> beam.io.ReadFromText(input_path_3)
            | 'Add Insert Date' >> beam.ParDo(AddInsertDate())
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                'bwt-lear.earthquake_dataset.earthquake_daily_data_dataflow',
                schema='SCHEMA_AUTODETECT',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

