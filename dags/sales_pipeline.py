from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import boto3
import os

# ======================
# 1) Extract
# ======================
def extract():
    data = {
        "order_id": [1, 2, 3],
        "product": ["A", "B", "C"],
        "price": [100, 150, 200],
        "qty": [2, 1, 3]
    }
    df = pd.DataFrame(data)

    os.makedirs("/opt/airflow/tmp", exist_ok=True)
    df.to_csv("/opt/airflow/tmp/sales_raw.csv", index=False)

    print("📌 Extract Complete")
    return "/opt/airflow/tmp/sales_raw.csv"


# ======================
# 2) Transform
# ======================
def transform():
    df = pd.read_csv("/opt/airflow/tmp/sales_raw.csv")

    # create new column
    df["total"] = df["price"] * df["qty"]

    df.to_csv("/opt/airflow/tmp/sales_transformed.csv", index=False)
    print("📌 Transform Complete")

    return "/opt/airflow/tmp/sales_transformed.csv"


# ======================
# 3) Load to S3 (LocalStack)
# ======================
def load_to_s3():
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1"
    )

    bucket_name = "sales-bucket"

    # create bucket if not exists
    try:
        s3.create_bucket(Bucket=bucket_name)
    except:
        pass

    s3.upload_file(
        "/opt/airflow/tmp/sales_transformed.csv",
        bucket_name,
        "sales_transformed.csv"
    )

    print("📌 Upload to S3 Success")


# ======================
# 4) Notify
# ======================
def notify():
    print("🎉 Pipeline Success!")


# ======================
# DAG Setup
# ======================
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    t1 = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform_sales_data",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load_to_s3",
        python_callable=load_to_s3
    )

    t4 = PythonOperator(
        task_id="notify_success",
        python_callable=notify
    )

    t1 >> t2 >> t3 >> t4
