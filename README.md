# Healthcare Data Pipeline on GCP

## Overview
This project implements an end-to-end automated healthcare data ingestion pipeline using Google Cloud Platform:

- Pulls 10,000 healthcare records daily from the CDC API.
- Stores raw data in Google Cloud Storage.
- Loads data into a partitioned BigQuery table.
- Orchestrated fully by Cloud Composer (Airflow).
- Version controlled in GitHub.
- CI/CD ready using Jenkins (optional).
- Future expansion: Dataproc Spark transformations, curated tables.

## Architecture
**Flow:**
1. Cloud Composer DAG runs daily at 6 AM.
2. Python task calls the CDC Healthcare API.
3. Saves JSON → CSV → uploads to GCS (`gs://leapproject2/raw/...`)
4. GCSToBigQueryOperator loads file into `leapproject2.health_api.covid_cases_api`.
5. Table is DAY-partitioned (ingestion-time).
6. Optional: Dataproc Spark transforms raw → curated tables.

## Components
- Cloud Composer (Airflow) for orchestration
- GCS for raw file storage
- BigQuery for analytics storage
- Dataproc (optional)
- Jenkins (optional CI/CD)
- GitHub for source control

## Deployment Steps
### 1. Set project
\`\`\`bash
gcloud config set project leapproject2
\`\`\`

### 2. Create dataset (if needed)
\`\`\`bash
bq --location=US mk --dataset leapproject2:health_api
\`\`\`

### 3. Upload DAG to Composer
\`\`\`bash
COMPOSER_DAGS_BUCKET="gs://<your-composer-bucket>/dags"
gsutil cp dags/health_api_api_to_bq_dag.py "\$COMPOSER_DAGS_BUCKET/"
\`\`\`

### 4. Enable DAG in Airflow UI
DAG name: **health_api_to_bq_daily**

## BigQuery Queries
See: `sql/sample_queries.sql`

