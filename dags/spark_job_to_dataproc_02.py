from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta

PROJECT_ID = "prnproject477"
REGION = "asia-southeast1"
CLUSTER_NAME = "mycluster"
PYSPARK_FILE = "gs://prn-spark-bucket/scripts/spark_example.py"

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="spark_job_to_dataproc",
    start_date=datetime.now() - timedelta(days=1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
) as dag:

    spark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_FILE},
    }

    run_spark = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job=spark_job,
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_conn",
    )
