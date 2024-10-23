# Script Name: dataflow_ingest.py
# Script Description:
# This script defines an Apache Beam pipeline to fetch earthquake data from the USGS API
# and store it in Google Cloud Storage (GCS) using utility functions.
# Update Date: 21-10-2024
#################################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import config
import utils  # Importing the utility functions

if __name__ == '__main__':
    def run():
        # Define the options for the Dataflow pipeline
        options = PipelineOptions(
            project=config.GCP_PROJECT_ID,
            runner='DataflowRunner',
            temp_location=config.TEMP_LOCATION,
            region=config.REGION
        )

        url = config.EARTHQUAKE_DATA_URL  # URL for fetching earthquake data
        bucket_name = config.GCS_BUCKET_NAME  # Bucket name
        destination_blob_name = config.BRONZE_PATH  # Destination path in GCS

        with beam.Pipeline(options=options) as pipeline:
            # Step 1: Fetch data from API and upload to GCS
            data = utils.fetch_api_data(url)  # Fetch data using the utility function

            # Step 2: Create a PCollection with the fetched data
            pipeline | 'Create Data' >> beam.Create([data]) \
                    | 'Upload to GCS' >> beam.Map(lambda x: utils.upload_to_gcs(bucket_name, destination_blob_name, x))

        run()
