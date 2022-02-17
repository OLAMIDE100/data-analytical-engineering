#python libraries
import os
import logging
from datetime import datetime

#airflow libraries
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

#gcloud libraries
from google.cloud import storage

#pyarrow libraries
import pyarrow.csv as pv
import pyarrow.parquet as pq

#Variables
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

#filename
fhw_dataset_file = "fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
fhw_parquet_file = fhw_dataset_file.replace('.csv', '.parquet')


#downloading url
fhw_dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{fhw_dataset_file}"

#paths
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")



#functions
def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


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

#default argument for dag definition/declaration


#DAG declaration using context manager

with DAG(
    dag_id="fhw_data_ingestion_gcs_dag",
    schedule_interval='@monthly',
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 12, 1),
    tags=['mide'],
    catchup=True,
    max_active_runs=3
) as dag:

    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command=f"curl -sS {fhw_dataset_url} > {path_to_local_home}/{fhw_dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id = "format_to_parquet_task",
        python_callable = format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{fhw_dataset_file}"
        }
    )

    local_to_gcs_task = PythonOperator(
        task_id = "local_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET, 
            "object_name" :f"for_hire_vehicle/{fhw_parquet_file}",
            "local_file": f"{path_to_local_home}/{fhw_parquet_file}"
            
        }
    )
    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task