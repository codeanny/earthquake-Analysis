# Script Name: load_historical_data.py
# Script Description:
# This script sets up a Spark session, fetches earthquake data from the USGS API,
# and uploads the data to a specified Google Cloud Storage bucket using utility functions.
# Update Date: 21-10-2024
#################################################
import json
import os
import logging
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, split, current_timestamp
from pyspark.sql.types import StructType, StructField, DecimalType, LongType, StringType, IntegerType
import utils  # Ensure this module contains the required functions
from config import config

# Configure logging
logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # Define Spark session
    spark = SparkSession.builder.master('local[*]').appName('historical_data').getOrCreate()

    # Set the environment variable for Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.SERVICE_ACCOUNT

    # URL for fetching earthquake data
    url = config.EARTHQUAKE_DATA_URL

    # Fetch data from API
    try:
        response = utils.fetch_api_data(url)
        logging.info("Data fetched successfully from the API.")
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        exit(1)

    # GCS bucket information
    bucket_name = config.GCS_BUCKET_NAME

    # Upload the data to GCS(BRONZE_LAYER)
    try:
        utils.upload_to_gcs(bucket_name, config.BRONZE_PATH, response)
        logging.info(f"File uploaded to {config.BRONZE_PATH} in bucket {bucket_name}.")
    except Exception as e:
        logging.error(f"Error uploading to GCS: {e}")
        exit(1)

    # Reading back the data from GCS for further processing
    try:
        downloaded_data = utils.read_data_from_gcs(bucket_name, source_blob_name=config.BRONZE_PATH)
        logging.info("Downloaded data from GCS.")
    except Exception as e:
        logging.error(f"Error reading from GCS: {e}")
        exit(1)

    # Flattening and processing the data using PySpark
    features = response['features']
    formatted_properties = []

    for feature in features:
        geometric_response = {
            "longitude": Decimal(feature['geometry']['coordinates'][0]),  # Convert to Decimal
            "latitude": Decimal(feature['geometry']['coordinates'][1]),  # Convert to Decimal
            "depth": Decimal(feature['geometry']['coordinates'][2])  # Convert to Decimal
        }

        properties_response = feature['properties']
        properties_response['geometry'] = geometric_response

        formatted_property = {
            'mag': Decimal(properties_response.get('mag', 0) or 0),
            'place': properties_response.get('place', ''),
            'time': properties_response.get('time', 0),
            'updated': properties_response.get('updated', 0),
            'tz': int(properties_response.get('tz', 0) or 0),  # Convert to int
            'url': properties_response.get('url', ''),
            'detail': properties_response.get('detail', ''),
            'felt': int(properties_response.get('felt', 0) or 0),  # Convert to int
            'cdi': Decimal(properties_response.get('cdi', 0) or 0),
            'mmi': Decimal(properties_response.get('mmi', 0) or 0),
            'alert': properties_response.get('alert', ''),
            'status': properties_response.get('status', ''),
            'tsunami': int(properties_response.get('tsunami', 0) or 0),  # Convert to int
            'sig': int(properties_response.get('sig', 0) or 0),  # Convert to int
            'net': properties_response.get('net', ''),
            'code': properties_response.get('code', ''),
            'ids': properties_response.get('ids', ''),
            'sources': properties_response.get('sources', ''),
            'types': properties_response.get('types', ''),
            'nst': int(properties_response.get('nst', 0) or 0),  # Convert to int
            'dmin': Decimal(properties_response.get('dmin', 0) or 0),  # Keep as Decimal
            'rms': Decimal(properties_response.get('rms', 0) or 0),
            'gap': Decimal(properties_response.get('gap', 0) or 0),  # Keep as Decimal
            'magType': properties_response.get('magType', ''),
            'type': properties_response.get('type', ''),
            'geometry': {
                'longitude': geometric_response['longitude'],
                'latitude': geometric_response['latitude'],
                'depth': geometric_response['depth']
            }
        }

        formatted_properties.append(formatted_property)

    # Define schema for the DataFrame
    input_schema = StructType([
        StructField("mag", DecimalType(10, 2), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", DecimalType(10, 2), True),
        StructField("mmi", DecimalType(10, 2), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", DecimalType(10, 2), True),
        StructField("rms", DecimalType(10, 2), True),
        StructField("gap", DecimalType(10, 2), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("geometry", StructType([
            StructField("longitude", DecimalType(10, 6), True),
            StructField("latitude", DecimalType(10, 6), True),
            StructField("depth", DecimalType(10, 6), True)
        ]), True)
    ])

    # Creating DataFrame from the structured data
    df = spark.createDataFrame(formatted_properties, schema=input_schema)
    # df.show(10)

    # Write DataFrame to GCS in json format
    # destination_blob_name_silver = 'silver/20241021/data.json'
    json_data = df.toJSON().collect()  # Collects the DataFrame as a list of JSON strings
    data_to_upload = [json.loads(record) for record in json_data]  # Convert each string to a dictionary

    # Transformation1: Convert epoch to timestamp
    df = df.withColumn("time", from_unixtime(col("time") / 1000)) \
        .withColumn("updated", from_unixtime(col("updated") / 1000))

    # Transformation2: Extract area from the "place" column based on the word "of"
    df = df.withColumn("area", split(col("place"), " of ").getItem(1))

    # Upload the data to GCS(BRONZE_LAYER)
    try:
        utils.upload_to_gcs(bucket_name, config.SILVER_PATH, data_to_upload)
        logging.info(f"File uploaded to {config.SILVER_PATH} in bucket {bucket_name}.")
    except Exception as e:
        logging.error(f"Error uploading to GCS: {e}")
        exit(1)

    # Upload to BigQuery:
    # Transformation1:

    df = df.withColumn("insert_dt", current_timestamp())

    df.show()
