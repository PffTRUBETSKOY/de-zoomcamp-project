import zipfile
from datetime import datetime
import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = 'https://info.stackoverflowsolutions.com/rs/719-EMH-566/images' 
URL_TEMPLATE = URL_PREFIX + '/stack-overflow-developer-survey-{{ execution_date.strftime(\'%Y\') }}.zip'
UNZIP_FILE_TEMPLATE = "{{ execution_date.strftime(\'%Y\') }}"
OUTPUT_FILE_TEMPLATE = 'developer_survey_{{ execution_date.strftime(\'%Y\') }}.zip'
PARQUET_FILE_TEMPLATE = 'developer_survey_{{ execution_date.strftime(\'%Y\') }}.parquet'
TABLE_NAME_TEMPLATE = 'developer_survey_{{ execution_date.strftime(\'%Y\') }}'
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'developer_survey')

DATASET = "developer_survey"
INPUT_PART = "final_raw"
INPUT_FILETYPE = "PARQUET"


def format_to_parquet(src_file, result, year):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pd.read_csv(src_file)
    table['year'] = int(year)
    table.to_parquet(result, engine='pyarrow')



def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def unzip_function(src_file, result):
    print('{{ execution_date.strftime(\'%Y\') }}')
    if not src_file.endswith('.zip'):
        logging.error("Can only accept source files in ZIP format, for the moment")
        return
    zipfile.ZipFile(src_file).extractall(result)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "catchup": True
}

with DAG(    
        dag_id="stackoverflow_to_bq_dag",
        schedule_interval="0 0 1 1 *",
        start_date=datetime(2017, 1, 1),
        end_date=datetime(2021, 1, 1),
        default_args=default_args,
        tags=['dtc-final'],
    ) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}"
    )

    unzip_task = PythonOperator(
        task_id="unzip_dataset_task",
        python_callable=unzip_function,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}",
            "result": f"{AIRFLOW_HOME}/{UNZIP_FILE_TEMPLATE}",
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_HOME}/{UNZIP_FILE_TEMPLATE}/survey_results_public.csv",
            "result": f"{AIRFLOW_HOME}/{UNZIP_FILE_TEMPLATE}/{PARQUET_FILE_TEMPLATE}",
            "year": UNZIP_FILE_TEMPLATE,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"final_raw/{PARQUET_FILE_TEMPLATE}",
            "local_file": f"{AIRFLOW_HOME}/{UNZIP_FILE_TEMPLATE}/{PARQUET_FILE_TEMPLATE}",
        },
    )


    cleanup_dataset_task = BashOperator(
        task_id="cleanup_dataset_task",
        bash_command=f"rm -rf {AIRFLOW_HOME}/{UNZIP_FILE_TEMPLATE} {AIRFLOW_HOME}/{OUTPUT_FILE_TEMPLATE}"
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "dev_survey_external_{{ execution_date.strftime(\'%Y\') }}",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE}",
                "sourceUris": [f"gs://{BUCKET}/" + "final_raw/developer_survey_{{ execution_date.strftime(\'%Y\') }}.parquet"],
            },
        },
    )


    download_dataset_task >> unzip_task >> format_to_parquet_task >> local_to_gcs_task >>  cleanup_dataset_task >> bigquery_external_table_task