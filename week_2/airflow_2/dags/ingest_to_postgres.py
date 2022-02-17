#python libraries
import os
from datetime import datetime
from ingest_data import load_data

#airflow libraries
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



host = os.getenv('PG_HOST')
password = os.getenv('PG_PASSWORD')
user = os.getenv('PG_USER')
port = os.getenv('PG_PORT')
db = os.getenv('PG_DB')




dataset_file = "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
dataset_url = f"https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
input_file = f'{path_to_local_home}/{dataset_file}'
table_name = "yellow_tripdata_{{execution_date.strftime(\'%Y_%m\')}}"



    
 

#DAG declaration using context manager
work_flow = DAG(
    dag_id="data_ingestion_to_postgres",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 2),
    end_date=datetime(2021, 2, 3)
)

with work_flow:

    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    load_to_postgres_task = PythonOperator(
        task_id = "load_to_postgres_task",
        python_callable=load_data,
        op_kwargs=dict(
            user=user,
            password=password,
            host=host,
            port=port,
            db=db,
            table_1=table_name,
            csv_name_1=input_file
        )
        
    )


    
    download_dataset_task >> load_to_postgres_task 

