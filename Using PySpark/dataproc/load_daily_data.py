import json
import os
from datetime import datetime
import requests
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col, regexp_extract, current_timestamp, count, avg, month, year, dayofmonth, to_timestamp
from pyspark.sql.types import StructField, IntegerType, StringType, DoubleType, LongType, StructType, Row
from utils import fetch_data_from_api, upload_to_gcs, read_data_from_gcs, flatten

if __name__ == "__main__":
    # Define spark session
    spark = SparkSession.builder.master("local[*]").appName('Daily Load').getOrCreate()

    # initialization of google application
    # os.environ[
    #     'GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Aniket Ahire\Downloads\bwt-lear-68d6af1da6d5.json"

    # ------------------------------------------------------------------------------------------------------------------

    current_date = datetime.now().strftime("%Y%m%d")  # Format the date as YYYYMMDD

    # Pulling data from API
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
    bucket_name = "earthquake_data_anny"
    destination_blob_name = f"daily_data/dataproc/landing/{current_date}/daily_data.json"

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
                                    source_blob_name=f"daily_data/dataproc/landing/{current_date}/daily_data.json")
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
    destination_blob_name = f'daily_data/dataproc/silver/{current_date}/daily_data.json'
    bucket_name = 'earthquake_data_anny'

    df.coalesce(1).write.mode('overwrite').parquet(f'gs://{bucket_name}/{destination_blob_name}')
    print(f"Data written to gs://{bucket_name}/{destination_blob_name} in Parquet format.")

    # # # ------------------------------------------------------------------------------------------------------------

    # Step1 : comment credentials to the main file & supportive file
    # Step2 : Run dataproc submit command on the SDK
    # Read data from silver.parquet file
    parquet_file_path = f"gs://earthquake_data_anny/daily_data/dataproc/silver/20241028/daily_data.*"

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
        .option("table", "bwt-lear.earthquake_dataset.earthquake_load-dailydata-Dataproc") \
        .option("writeMethod", "direct") \
        .save()

    #  =======================================================================================================================

    ############analysis#################

    # 1. Count the number of earthquakes by region

    df = df1.filter(col('type') == 'earthquake').groupby('area').count()
    # df.show()

    ##2. Find the average magnitude by the region
    df_avg = df1.groupby('area').agg(avg('mag').alias('average_magnitude'))
    # df.show()

    ##3. Find how many earthquakes happen on the same day.

    df1 = df1.withColumn('time', to_timestamp(col('time'), 'yyyy-MM-dd HH:mm:ss'))

    # Extract year, month, and day, count the number of earthquakes
    df_result = (
        df1.withColumn('year', year(col('time')))
        .withColumn('month', month(col('time')))
        .withColumn('day', dayofmonth(col('time')))
        .groupby('year', 'month', 'day')
        .agg(count('type').alias('earthquake_count'))
        .orderBy('year', 'month', 'day')
    )

    # Show the result
    # df_result.show()
    ####4. Find how many earthquakes happen on same day and in same region

    daily_counts_area = (
        df1.withColumn('year', year(col('time')))
        .withColumn('month', month(col('time')))
        .withColumn('day', dayofmonth(col('time')))
        .groupBy('year', 'month', 'day', 'area')
        .agg(count('type').alias('earthquake_count'))
    )

    # Calculate average earthquakes per day and area
    average_daily_counts_area = (
        daily_counts_area.groupBy('year', 'month', 'day', 'area')
        .agg(avg('earthquake_count').alias('average_earthquakes_per_day'))
    )

    # average_daily_counts_area.show()
    ###5. Find average earthquakes happen on the same day.

    # Check if 'time' column exists and is of type timestamp

    # Filter for earthquakes in the last week and find the highest magnitude per region
    highest_magnitude_region = (
        df1.filter(col('time') >= (current_timestamp() - expr("INTERVAL 7 DAYS")))  # Filter for last week
        .groupBy('area')  # Group by region
        .agg(max('mag').alias('highest_magnitude'))  # Aggregate to find max magnitude
        .orderBy(col('highest_magnitude').desc())  # Order by magnitude in descending order
        .limit(1)  # Limit to one result
    )

    # highest_magnitude_region.show()
    ###6. Find average earthquakes happen on same day and in same region

    frequency_intensity = (
        df1.groupBy('area')
        .agg(
            count('*').alias('earthquake_count'),
            avg('mag').alias('average_magnitude'),
            avg('mmi').alias('average_intensity')
        )
        .orderBy(col('earthquake_count').desc(), col('average_magnitude').desc())
    )

    # frequency_intensity.show()

    ##7. Find the region name, which had the highest magnitude earthquake last week.
    highest_magnitude_region = (
        df1.filter(col('time') >= (current_timestamp() - expr("INTERVAL 7 DAYS")))  # Filtering for last week
        .groupBy('area')  # Group by region
        .agg(max(col('mag')).alias('highest_magnitude'))  # Aggregate to find max magnitude
        .orderBy(col('highest_magnitude').desc())  # Order by magnitude in descending order
        .limit(1)  # Limit to one result
    )

    # highest_magnitude_region.show()

    ##8. Find the region name, which is having magnitudes higher than 5.
    regions_above_five = (
        df1.filter(col('mag') > 5)  # Filtering for magnitudes greater than 5
        .select('area', 'mag')  # Selecting the area and magnitude
    )

    # regions_above_five.show()
    ####9. Find out the regions which are having the highest frequency and intensity of earthquakes.
    highest_frequency_intensity = (
        df1.groupBy('area')  # Group by region
        .agg(
            count("*").alias('earthquake_count'),  # Count of earthquakes
            avg('mag').alias('average_magnitude'),  # Average magnitude
            avg('mmi').alias('average_intensity')  # Average intensity
        )
        .orderBy(col('earthquake_count').desc(), col('average_magnitude').desc())
        # Order by frequency and magnitude
    )

    highest_frequency_intensity.show()
