from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common_funcs import parquetize, upload_to_gcs
from airflow import DAG
from datetime import datetime
import os
from dateutil.relativedelta import relativedelta


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

dataset_file = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
dataset_file = dataset_file.replace('.csv', '.parquet')
URL_PREFIX = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
URL_TEMPLATE =  URL_PREFIX + 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'


# fhv_dag = DAG(
#     "fhv_to_gcs",
#     start_date=datetime(2019, 1, 3),
#     end_date=datetime(2019, 12, 3),
#     schedule_interval="0 6 3 * *"
#     )

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="fhv_to_gcs",
    start_date=datetime(2019, 1, 3),
    end_date= datetime(2019, 1, 3) + relativedelta(months=+12),
    schedule_interval="0 6 3 * *",
    tags=['dtc-de'],
    default_args=default_args,
    max_active_runs=2,
    catchup=True
) as fhv_dag:

    curl_task = BashOperator(
        task_id = 'curl',
        bash_command= f'curl -sSlf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    parquetize_task = PythonOperator(
        task_id = 'parquetize',
        python_callable=parquetize,
        op_kwargs={
        'file_': OUTPUT_FILE_TEMPLATE
        },
    )

    upload_gcs_task= PythonOperator(
        task_id = "upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket":BUCKET,
            "path": f"raw/{dataset_file}",
            "file_": f"{AIRFLOW_HOME}/{dataset_file}"
        },
    )

    # upload_BigQ = BigQueryCreateExternalTableOperator(
    #     task_id='upload_big_query',
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
    #         },
    #     },
    # )

    rm_temp_task = BashOperator(
    task_id = 'delete_files',
    bash_command= f'rm {OUTPUT_FILE_TEMPLATE} {AIRFLOW_HOME}/{dataset_file}'
    )

curl_task >> parquetize_task >> upload_gcs_task  >> rm_temp_task