from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def connect_airflow():
    print("Conntect Success!!")

with DAG(
    dag_id="hello_mwaa",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"]
):
    hello = PythonOperator(
        task_id="connect_airflow",
        python_callable=connect_airflow
    )

