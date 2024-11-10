import json
import os
from datetime import datetime
import requests
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, regexp_extract, current_timestamp
from pyspark.sql.types import StructField, IntegerType, StringType, DoubleType, LongType, StructType, Row
from utils import fetch_data_from_api, upload_to_gcs, read_data_from_gcs, flatten


if __name__ == "__main__":

    # Define spark session
    spark = SparkSession.builder.master("local[*]").appName('Historical Load').getOrCreate()

    # initialization of google application
    # os.environ[
    #     'GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

# ------------------------------------------------------------------------------------------------------------------

    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD

    # Pulling data from API
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    bucket_name = "earthquake_data_anny"
    destination_blob_name = f"historical_data/dataproc/landing/{current_date}/historical_data.json"

    # Using python request lib, fetching data from G iven URL
    response = fetch_data_from_api(url)
    # print(type(response))

# ----------------------------------------------------------------------------------------------------------------

    # Fetch the data from the API
    data = response
    # Upload the data to GCS
    upload_to_gcs(bucket_name=bucket_name, destination_blob_name=destination_blob_name, data=data)

# -----------------------------------------------------------------------------------------------------------------

    # Read data from gcs
    input_file = read_data_from_gcs(bucket_name="earthquake_data_anny",
                                         source_blob_name=f"historical_data/dataproc/landing/{current_date}/historical_data.json")
    # print(input_file)

    flatten = flatten(spark, input_file)
    # flatten.show(truncate=False)


# --------------------------------------------------------------------------------------------------------------------

    # Convert 'time' and 'updated' from milliseconds (epoch) to timestamp
    df = flatten.withColumn('time', from_unixtime(col('time') / 1000)) \
        .withColumn('updated', from_unixtime(col('updated') / 1000))

    # Define the regex pattern to extract everything after "of"
    pattern = r'of\s(.*)'

    # Create the new "area" column by extracting the text after "of"
    df = df.withColumn('area', regexp_extract(col('place'), pattern, 1))
    # df.show(truncate=False)


# # --------------------------------------------------------------------------------------------------------------

    # upload data into GSC in the form of parquet
    # Define destination path for Parquet file
    destination_blob_name = f'historical_data/dataproc/silver/{current_date}/historical_data.json'
    bucket_name = 'earthquake_data_anny'

    df.coalesce(1).write.mode('overwrite').parquet(f'gs://{bucket_name}/{destination_blob_name}')
    print(f"Data written to gs://{bucket_name}/{destination_blob_name} in Parquet format.")

# # # ------------------------------------------------------------------------------------------------------------

    # Step1 : comment credentials to the main file & supportive file
    # Step2 : Run dataproc submit command on the SDK
    # Read data from silver.parquet file
    parquet_file_path = f"gs://earthquake_data_anny/historical_data/dataproc/silver/20241028/historical_data.*"

    # Read the Parquet file
    df = spark.read.parquet(parquet_file_path)
    df.show()

# # # ---------------------------------------------------------------------------------------------------------------

    # Insert data : insert_dt (Timestamp)

    df1 = df.withColumn('insert_date', current_timestamp())
    # df1.show(truncate=False)

# # # ---------------------------------------------------------------------------------------------------------------

    # Write data to bigquery
    df1.write.format("bigquery") \
        .option("table", "bwt-lear.earthquake_dataset.earthquake_load-historical-data-Dataproc") \
        .option("writeMethod", "direct") \
        .save()