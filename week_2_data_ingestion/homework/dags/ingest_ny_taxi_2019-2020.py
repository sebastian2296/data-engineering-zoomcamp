# What do I need? DAG class, bash, python and BigQuery Operator, ENVIRONMENT VARS, 
# URL_TEMPLATE, FILE_NAME, OUTPUT_TEMPLATE

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common_funcs import parquetize, upload_to_gcs
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os


# ENV 

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

# TEMPLATES 

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/' 
URL_TEMPLATE = URL_PREFIX + 'trip+data/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
data_file = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
OUTPUT_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'

ny_taxi = DAG(
    'upload_ny_taxi_data',
    start_date=datetime(2019, 1, 1),
    end_date= datetime(2019, 1, 1) + relativedelta(months=+24),
    schedule_interval="0 6 3 * *",
    max_active_runs=2, 
    catchup=True
)

with ny_taxi:

    wget_task = BashOperator(
        task_id = 'download_ny_taxi_data',
        bash_command = f'curl sSLf {URL_TEMPLATE} > {OUTPUT_TEMPLATE}'
    )

    parquetize_task = PythonOperator(
        task_id = 'change_csv_to_parquet',
        python_callable = parquetize,
        op_kwargs = {
            'file_': OUTPUT_TEMPLATE
        }
    )

    upload_gcs_task = PythonOperator(
        task_id = 'upload_parquet_data_to_gcs',
        python_callable = upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "path": f'raw/{data_file}',
            "file_": f"{AIRFLOW_HOME}/{data_file}"
        }
    )

    # upload_big_query_task = BigQueryCreateExternalTableOperator(
    #     task_id='upload_big_query',
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{data_file}"],
    #         },
    #     },
    # )

    rm_temp = BashOperator(
        task_id = "remove_temporary_files",
        bash_command = f'rm {OUTPUT_TEMPLATE} {AIRFLOW_HOME}/{data_file}'
    )

wget_task >> parquetize_task >> upload_gcs_task >> rm_temp
