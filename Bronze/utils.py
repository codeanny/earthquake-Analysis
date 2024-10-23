# Script Name: utils.py
# Script Description:
# This script contains utility functions for fetching data from an API and uploading it to Google Cloud Storage.
# The utils.py file is designed to store utility functions that can be reused across different scripts in our project
# Update Date: 21-10-2024
#################################################

import json
import requests
from google.cloud import storage


# calling the fetch_api_data function from utils.py
def fetch_api_data(url):  # This function sends a request to a given URL and retrieves data, it also make sure data returned should be in JSON format.
    """
    Fetch data from an API using the requests' library.

    :param url: API URL to fetch data from
    :return: Parsed JSON object as a Python dictionary or list
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()  # Converts the response from a JSON string to a Python dictionary or list,

def upload_to_gcs(bucket_name, destination_blob_name, data):
    """
    Upload JSON data to a Google Cloud Storage bucket.

    :param bucket_name: Name of the GCS bucket
    :param destination_blob_name: Name for the uploaded file in the bucket
    :param data: Data to upload (Python dictionary or list)
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Convert the data to a JSON string
    json_data = json.dumps(data)  # Converts the Python dictionary or list to a JSON string.

    # Here We can Upload the JSON string format data to the GCS bucket
    blob.upload_from_string(json_data, content_type='application/json')  # content type make sure that data is recognized as JSON.

    print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")

    # read data from GCS Bucket


def read_data_from_gcs(bucket_name, source_blob_name):
    """
    :param bucket_name:
    :param source_blob_name:
    :return:
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Download the JSON String

    json_data = blob.download_as_text()
    return json.loads(json_data)  # Converts the JSON string back to a dictionary
