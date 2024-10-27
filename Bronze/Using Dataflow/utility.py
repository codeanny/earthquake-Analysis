import requests
import logging
import json

from apache_beam import Row
from google.cloud import storage


def fetch_api_data(url):
    """Fetch data from the provided API URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
        raise
    except Exception as err:
        logging.error(f"An error occurred: {err}")
        raise


def upload_to_gcs(bucket_name, destination_blob_name, data):
    """Upload data to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Ensure data is JSON formatted
    if isinstance(data, dict) or isinstance(data, list):
        data = json.dumps(data)

    blob.upload_from_string(data)
    logging.info(f"Uploaded data to {destination_blob_name} in bucket {bucket_name}.")


def read_from_gcs(bucket_name, source_blob_name):
    """Read data from Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    data = blob.download_as_text()
    logging.info(f"Read data from {source_blob_name} in bucket {bucket_name}.")
    return data


def flatten_feature(features):
    """
    Flatten a GeoJSON feature into a Row object.

    Args:
        features (dict): A GeoJSON feature collection.

    Returns:
        list: A list of Row representations of the flattened features.
    """
    flattened_records = []

    for feature in features.get('features', []):
        try:
            # Use a helper function to safely convert values to float
            def safe_float(value, default=0.0):
                if value is None:
                    return default
                return float(value)

            flattened_record = Row(
                id=feature['id'],
                mag=safe_float(feature['properties'].get('mag')),  # Default to 0.0 if None
                place=feature['properties'].get('place', 'Unknown'),  # Default to 'Unknown' if None
                time=feature['properties'].get('time'),  # May be None, check how to handle it
                updated=feature['properties'].get('updated'),  # May be None
                url=feature['properties'].get('url'),  # Optional field
                detail=feature['properties'].get('detail'),  # Optional field
                felt=safe_float(feature['properties'].get('felt', 0)),  # Default to 0 if not present
                cdi=safe_float(feature['properties'].get('cdi')),  # Default to 0.0 if None
                mmi=float(feature['properties'].get('mmi')) if feature['properties'].get('mmi') is not None else None,
                alert=feature['properties'].get('alert'),  # Optional field
                status=feature['properties']['status'],  # Must exist
                tsunami=int(feature['properties']['tsunami']),  # Assuming this should be an integer
                sig=feature['properties']['sig'],  # Optional field
                net=feature['properties']['net'],  # Optional field
                code=feature['properties']['code'],  # Optional field
                ids=feature['properties'].get('ids'),  # Optional field
                sources=feature['properties'].get('sources'),  # Optional field
                types=feature['properties'].get('types'),  # Optional field
                nst=feature['properties'].get('nst'),  # Optional field
                dmin=safe_float(feature['properties'].get('dmin')),  # Default to 0.0 if None
                rms=safe_float(feature['properties'].get('rms')),  # Optional field
                gap=safe_float(feature['properties'].get('gap')),  # Default to 0.0 if None
                magType=feature['properties']['magType'],  # Must exist
                type=feature['properties']['type'],  # Must exist
                title=feature['properties']['title'],  # Must exist
                tz=feature['properties'].get('tz'),  # Optional field
                longitude=safe_float(feature['geometry']['coordinates'][0]),  # Ensure longitude is float
                latitude=safe_float(feature['geometry']['coordinates'][1]),  # Ensure latitude is float
                depth=safe_float(feature['geometry']['coordinates'][2])  # Explicitly cast depth to float
            )
            flattened_records.append(flattened_record)
        except KeyError as e:
            logging.error(f"Missing essential field in feature: {e}")  # Handle missing fields
        except TypeError as e:
            logging.error(f"Type error encountered: {e}")  # Log TypeError

    return flattened_records