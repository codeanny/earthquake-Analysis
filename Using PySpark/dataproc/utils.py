import json
import os

import requests
from google.cloud import storage
from pyspark.sql.types import StructType, DoubleType, StructField, LongType, StringType, IntegerType, Row

# os.environ[
#         'GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

client = storage.Client(project="earthquake-project-439312")


def fetch_data_from_api(api_url):
    """
    Fetch data from a given API endpoint.

    :param api_url: URL of the API to fetch data from.
    :return: Parsed JSON data from the API response.
    """
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}")


# --------------------------------------------------------------------------------------------------------------------

# Load data into from API to bucket

def upload_to_gcs(bucket_name, destination_blob_name, data):
    """Uploads JSON data to a Google Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert the dictionary to a JSON string
    json_data = json.dumps(data)  # Converts data to a JSON string

    # Upload the JSON string directly
    blob.upload_from_string(json_data, content_type='application/json')
    # print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")


# -------------------------------------------------------------------------------------------------------------------

# Define the function to read data from GCS
def read_data_from_gcs(bucket_name, source_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download the blob's content as text
    data = blob.download_as_text()
    print("Data from GCS:")
    # print(data)  # Print the raw data to check its format
    return json.loads(data)  # Ensure this returns a dict


# ------------------------------------------------------------------------------------------------------------
def flatten(spark, geojson_data):
    """
    Convert GeoJSON data to a PySpark DataFrame.

    Args:
        geojson_data (dict): A dictionary representing GeoJSON data.

    Returns:
        pyspark.sql.DataFrame: A PySpark DataFrame containing the flattened GeoJSON data.
    """
    # Define the schema explicitly
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", DoubleType(), True),
        StructField("mmi", DoubleType(), True),
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
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("gap", DoubleType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
        StructField("tz", IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth", DoubleType(), True)
    ])

    # Prepare data for PySpark DataFrame
    rows_to_insert = []
    for feature in geojson_data['features']:
        flattened_record = Row(
            id=feature['id'],
            mag=float(feature['properties']['mag']),
            place=feature['properties']['place'],
            time=feature['properties']['time'],
            updated=feature['properties']['updated'],
            url=feature['properties']['url'],
            detail=feature['properties'].get('detail'),  # Optional field
            felt=feature['properties'].get('felt', 0),  # Default to 0 if not present
            cdi=float(feature['properties'].get('cdi', 0.0)) if feature['properties'].get('cdi') is not None else 0.0,
            # Ensure cdi is float, default to 0.0 if None
            mmi=float(feature['properties'].get('mmi')) if feature['properties'].get('mmi') is not None else None,
            alert=feature['properties'].get('alert'),  # Optional field
            status=feature['properties']['status'],
            tsunami=feature['properties']['tsunami'],
            sig=feature['properties']['sig'],
            net=feature['properties']['net'],
            code=feature['properties']['code'],
            ids=feature['properties'].get('ids'),  # Optional field
            sources=feature['properties'].get('sources'),  # Optional field
            types=feature['properties'].get('types'),  # Optional field
            nst=feature['properties'].get('nst'),  # Optional field
            dmin=float(feature['properties'].get('dmin', 0.0)) if feature['properties'].get('dmin') is not None else 0.0,  # Ensure dmin is float
            rms=float(feature['properties'].get('rms')),  # Optional field
            gap=float(feature['properties'].get('gap', 0.0)) if feature['properties'].get('gap') is not None else 0.0,  # Default to 0.0 if None
            magType=feature['properties']['magType'],
            type=feature['properties']['type'],
            title=feature['properties']['title'],
            tz=feature['properties'].get('tz'),  # Include tz field
            longitude=float(feature['geometry']['coordinates'][0]),  # Ensure longitude is float
            latitude=float(feature['geometry']['coordinates'][1]),  # Ensure latitude is float
            depth=float(feature['geometry']['coordinates'][2])  # Explicitly cast depth to float
        )
        rows_to_insert.append(flattened_record)

    # Create PySpark DataFrame
    df = spark.createDataFrame(rows_to_insert, schema)
    return df
