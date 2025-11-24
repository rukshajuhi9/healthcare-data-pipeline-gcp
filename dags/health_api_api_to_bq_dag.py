from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import requests
from google.cloud import storage

PROJECT_ID = "leapproject2"
DATASET = "health_api"
BUCKET = "leapproject2"
API_URL = "https://data.cdc.gov/resource/n8mc-b4w4.json"
LIMIT = 10000

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_api_to_gcs(**context):
    params = {"$limit": LIMIT}
    resp = requests.get(API_URL, params=params)
    data = resp.json()

    # Local temp file
    local_path = "/tmp/api_data.csv"

    # Convert to CSV
    import csv
    with open(local_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET)
    blob = bucket.blob(f"raw/health_api_data_{context['ds']}.csv")
    blob.upload_from_filename(local_path)


with DAG(
    "health_api_to_bq_daily",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["health_api", "cdc", "gcs", "bq"],
) as dag:

    api_to_gcs = PythonOperator(
        task_id="api_to_gcs",
        python_callable=fetch_api_to_gcs,
        provide_context=True,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=BUCKET,
        source_objects=["raw/health_api_data_{{ ds }}.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.covid_cases_api",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        time_partitioning={"type": "DAY"},
    )

    api_to_gcs >> load_to_bq
