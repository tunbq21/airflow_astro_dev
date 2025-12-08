# import datetime
# from airflow import DAG
# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# PROJECT_ID = "prnproject477"
# REGION = "asia-southeast1"
# CLUSTER_NAME = "mycluster"

# DEFAULT_ARGS = {
#     "owner": "airflow",
#     "start_date": datetime.datetime(2024, 1, 1),
# }

# # Spark job mẫu: chạy script hello-world có sẵn trong GCS
# PYSPARK_URI = "gs://dataproc-examples/pyspark/hello-world/hello-world.py"

# with DAG(
#     dag_id="spark_job_dataproc_example",
#     schedule=None,
#     default_args=DEFAULT_ARGS,
#     catchup=False,
# ) as dag:

#     run_spark = DataprocSubmitJobOperator(
#         task_id="run_spark_job",
#         project_id=PROJECT_ID,
#         region=REGION,
#         gcp_conn_id="google_cloud_default",
#         job={
#             "placement": {"cluster_name": CLUSTER_NAME},
#             "pyspark_job": {
#                 "main_python_file_uri": PYSPARK_URI
#             },
#         },
#     )
