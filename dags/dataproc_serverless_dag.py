from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# --- Cấu hình của bạn (THAY THẾ CÁC GIÁ TRỊ NÀY) ---
PROJECT_ID = "PRNPROJECT477"  
REGION = "us-central1"
GCS_BUCKET = "your-unique-airflow-bucket-2025"  
GCP_CONN_ID = "google_cloud_default" 
SPARK_JOB_FILE_NAME = "spark_word_count.py"
# ------------------------

# Cấu hình Batch Job cho PySpark
BATCH_CONFIG = {
    "pyspark_batch": {
        # Đường dẫn tệp PySpark trên GCS (tệp này được upload tự động sau)
        "main_python_file_uri": f"gs://{GCS_BUCKET}/jobs/{SPARK_JOB_FILE_NAME}",
        # Đối số cho tệp PySpark: Output Path
        "args": [
            f"gs://{GCS_BUCKET}/dataproc-output/serverless-result-{{{{ ds_nodash }}}}"
        ],
    },
    "runtime_config": {
        # Cấu hình cơ bản cho Dataproc Serverless
        "version": "1.1" 
    },
    "environment_config": {
        # Cấu hình Network (thường không cần thiết trừ khi có VPC tùy chỉnh)
    }
}


with DAG(
    dag_id="dataproc_serverless_spark_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dataproc", "serverless"],
) as dag:
    
    # Airflow sẽ tự động upload tệp job từ thư mục 'include/' lên GCS 
    # trong quá trình chạy DAG nếu bạn cấu hình đúng. 
    # Tuy nhiên, để đơn giản nhất, ta giả định tệp đã được upload thủ công hoặc sử dụng
    # LocalFilesystemToGCSOperator như ví dụ trước.

    # 1. Gửi và chạy Batch Job (Serverless)
    create_serverless_batch = DataprocCreateBatchOperator(
        task_id="create_spark_batch",
        project_id=PROJECT_ID,
        region=REGION,
        batch=BATCH_CONFIG,
        # Tạo ID batch dựa trên ngày chạy
        batch_id=f"serverless-spark-job-{{{{ ds_nodash }}}}", 
        gcp_conn_id=GCP_CONN_ID,
    )