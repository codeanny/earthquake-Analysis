from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os

# Set up DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'earthquake_data_pipeline',
    default_args=default_args,
    description='ETL pipeline to process earthquake data using Spark on Dataproc',
    schedule_interval='@daily',
)

# Define paths and variables
project_id = 'bwt-lear'
bucket_name = 'earthquake_data_anny'
region = 'us-central1'
cluster_name = 'dataproc-cluster'
gcs_landing_path = f'gs://{bucket_name}/daily_data/dataproc/landing/{datetime.now().strftime("%Y%m%d")}/daily_data.json'
gcs_silver_path = f'gs://{bucket_name}/daily_data/dataproc/silver/{datetime.now().strftime("%Y%m%d")}/daily_data.parquet'
bigquery_table = f'{project_id}.earthquake_dataset.earthquake_load_dailydata_Dataproc'

# Dataproc job configuration
pyspark_job = {
    'reference': {'project_id': project_id},
    'placement': {'cluster_name': cluster_name},
    'pyspark_job': {
        'main_python_file_uri': 'gs://path_to_your_script/your_pyspark_script.py',
        'args': [
            '--bucket_name', bucket_name,
            '--gcs_landing_path', gcs_landing_path,
            '--gcs_silver_path', gcs_silver_path,
            '--bigquery_table', bigquery_table,
        ],
    },
}

# Task 1: Submit Dataproc Job to Run PySpark Script
submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=pyspark_job,
    region=region,
    project_id=project_id,
    dag=dag,
)

# Task 2: Transfer Parquet Data from GCS (Silver Zone) to BigQuery
load_to_bigquery = GCSToBigQueryOperator(
    task_id='load_to_bigquery',
    bucket=bucket_name,
    source_objects=[f'daily_data/dataproc/silver/{datetime.now().strftime("%Y%m%d")}/daily_data.parquet'],
    destination_project_dataset_table=bigquery_table,
    source_format='PARQUET',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

# Set task dependencies
submit_pyspark_job >> load_to_bigquery
