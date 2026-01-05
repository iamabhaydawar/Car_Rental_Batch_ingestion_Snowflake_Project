from datetime import datetime, timedelta
import json
import os
from airflow import DAG
from airflow.sdk import Variable, Param
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator 
from google.cloud import dataproc_v1
from google.auth import default as google_auth_default
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

# GCS Bucket Configuration
GCS_BUCKET = "snowflake-projects-test-gds-bucket"  # Verify this is correct
SPARK_JOB_FILE = f"gs://{GCS_BUCKET}/car_rental_spark_job/spark_job.py"

# Default settings for all tasks in this DAG (owner, retries, etc.)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition: no schedule, manual trigger; accepts yyyymmdd param via Airflow params
dag = DAG(
    'car_rental_data_pipeline',
    default_args=default_args,
    description='Car Rental Data Pipeline',
    schedule=None,
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=['dev'],
    params={
        'execution_date': Param(default='NA', type='string', description='Execution date in yyyymmdd format'),
    }
)


# Python function to setup GCP credentials file from Airflow variable
def setup_gcp_credentials(**kwargs):
    """Write GCP credentials from GOOGLE_APPLICATION_CREDENTIALS variable to file"""
    adc_credentials_json = Variable.get('GOOGLE_APPLICATION_CREDENTIALS', deserialize_json=True)
    
    # Write credentials to file for ADC
    credentials_path = '/opt/airflow/gcp_adc_credentials.json'
    os.makedirs(os.path.dirname(credentials_path), exist_ok=True)
    
    with open(credentials_path, 'w') as f:
        json.dump(adc_credentials_json, f)
    
    # Set file permissions
    os.chmod(credentials_path, 0o600)
    
    return credentials_path

# Python function to submit Dataproc job with ADC credentials
def submit_dataproc_job_with_adc(**kwargs):
    """Submit Dataproc job using ADC credentials from environment variable"""
    # Ensure credentials file exists and set env var
    credentials_path = '/opt/airflow/gcp_adc_credentials.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
    
    # Get configuration
    project_id = Variable.get("GCP_PROJECT_ID")
    region = Variable.get("GCP_REGION", default="us-central1")
    cluster_name = Variable.get("DATAPROC_CLUSTER", default="cluster-8dec")
    execution_date = kwargs['ti'].xcom_pull(task_ids='get_execution_date')
    
    # Get credentials using ADC
    credentials, _ = google_auth_default()
    
    # Create Dataproc job controller client
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"},
        credentials=credentials
    )
    
    # Build job configuration
    job = dataproc_v1.Job()
    job.placement.cluster_name = cluster_name
    
    pyspark_job = dataproc_v1.PySparkJob()
    pyspark_job.main_python_file_uri = "gs://snowflake-projects-test-gds-bucket/car_rental_spark_job/spark_job.py"
    pyspark_job.args = [f"--date={execution_date}"]
    # JAR files must match Dataproc cluster Spark version (3.3) and Scala version (2.12)
    pyspark_job.jar_file_uris = [
        f"gs://{GCS_BUCKET}/snowflake_jars/spark-snowflake_2.12-2.11.1-spark_3.3.jar",
        f"gs://{GCS_BUCKET}/snowflake_jars/snowflake-jdbc-3.26.1.jar"
    ]
    job.pyspark_job = pyspark_job
    
    # Submit job
    parent = f"projects/{project_id}/regions/{region}"
    response = job_client.submit_job(
        project_id=project_id,
        region=region,
        job=job
    )
    
    return response.job_uuid

# Python function to compute effective execution date (param overrides ds)
def get_execution_date(ds_nodash, **kwargs):
    execution_date = kwargs['params'].get('execution_date', 'NA')
    if execution_date == 'NA':
        execution_date = ds_nodash
    return execution_date

# Task: Setup GCP credentials file from variable (runs first)
setup_gcp_credentials_task = PythonOperator(
    task_id='setup_gcp_credentials',
    python_callable=setup_gcp_credentials,
    dag=dag,
)

# Task: Resolve execution date and push to XCom
get_execution_date_task = PythonOperator(
    task_id='get_execution_date',
    python_callable=get_execution_date,
    op_kwargs={'ds_nodash': '{{ ds_nodash }}'},
    dag=dag,
)

# Task: Close out changed current records in SCD2 dimension (end_date, is_current=false)
merge_customer_dim = SQLExecuteQueryOperator(
    task_id='merge_customer_dim',
    conn_id='snowflake_conn',
    sql="""
        MERGE INTO car_rental.PUBLIC.customer_dim AS target
        USING (
            SELECT
                $1 AS customer_id,
                $2 AS name,
                $3 AS email,
                $4 AS phone
            FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv
        ) AS source
        ON target.customer_id = source.customer_id AND target.is_current = TRUE
        WHEN MATCHED AND (
            target.name != source.name OR
            target.email != source.email OR
            target.phone != source.phone
        ) THEN
            UPDATE SET target.end_date = CURRENT_TIMESTAMP(), target.is_current = FALSE;
    """,
    dag=dag,
)

# Task: Insert new version rows as current (effective_date now, end_date null)
insert_customer_dim = SQLExecuteQueryOperator(
    task_id='insert_customer_dim',
    conn_id='snowflake_conn',
    sql="""
        INSERT INTO car_rental.PUBLIC.customer_dim (customer_id, name, email, phone, effective_date, end_date, is_current)
        SELECT
            $1 AS customer_id,
            $2 AS name,
            $3 AS email,
            $4 AS phone,
            CURRENT_TIMESTAMP() AS effective_date,
            NULL AS end_date,
            TRUE AS is_current
        FROM @car_rental.PUBLIC.car_rental_data_stg/customers_{{ ti.xcom_pull(task_ids='get_execution_date') }}.csv;
    """,
    dag=dag,
)

# Get configuration from Airflow variables
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
REGION = Variable.get("GCP_REGION", default="us-central1")
CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default="cluster-8dec")

pyspark_job_file_path = 'gs://snowflake-projects-test-gds-bucket/car_rental_spark_job/spark_job.py'

# Task: Submit PySpark job to Dataproc using PythonOperator with ADC
# This ensures GOOGLE_APPLICATION_CREDENTIALS env var is set in the same process
submit_pyspark_job = PythonOperator(
    task_id='submit_pyspark_job',
    python_callable=submit_dataproc_job_with_adc,
    dag=dag,
)

# Orchestration: setup credentials -> date -> SCD2 close -> insert current -> run Spark job
setup_gcp_credentials_task >> get_execution_date_task >> merge_customer_dim >> insert_customer_dim >> submit_pyspark_job
