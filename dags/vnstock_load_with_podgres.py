from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta    
import pandas as pd
from vnstock import Vnstock
import os  
import psycopg2 
import psycopg2.sql as sql
import psycopg2.extras as execute_values 
import airflow.hooks.base as base_hook  

# --- 1. Cấu hình DAG và Biến Cục bộ ---
default_args = {
    'owner': 'Tuan Quang', 
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email': ['tbuiquang103@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,    
    'retries': 1,
    'retry_delay': timedelta(minutes=10),   
}

BASE_DIR = "/usr/local/airflow/dags/data_lake/vnstock_prices_csv"

airflow_conn = base_hook.BaseHook.get_connection("local_postgres")

# conn = pyscopg2.connect(
#     host="your_host",
#     database="your_database",
#     user="your_user",
#     password="your_password"
# )

# conn = psycopg2.connect(
#     host=airflow_conn.host,
#     database=airflow_conn.schema,
#     user=airflow_conn.login,
#     password=airflow_conn.password
# )

# cur = conn.cursor()

# Date,open,high,low,close,volume,Ticker,year,month
# 2025-12-12,96.0,96.0,93.5,93.7,5519800,FPT,2025,12


def load_data_to_postgres(**kwargs):
    execution_date = kwargs["ds"]
    year, month, _ = execution_date.split("-")

    conn = psycopg2.connect(
        host=airflow_conn.host,
        database=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password
    )

    try:
        with conn:
            with conn.cursor() as cur:
                for symbol in os.listdir(BASE_DIR):
                    symbol_dir = os.path.join(BASE_DIR, symbol)

                    if not os.path.isdir(symbol_dir):
                        continue

                    file_path = os.path.join(
                        BASE_DIR, symbol, year, month, f"{execution_date}.csv"
                    )

                    if not os.path.exists(file_path):
                        print(f"Không có file cho {symbol} ngày {execution_date}")
                        continue

                    df = pd.read_csv(file_path)

                    table_name = f"vnstock_prices_{symbol.lower()}"

                    cur.execute(
                        sql.SQL("""
                            CREATE TABLE IF NOT EXISTS {} (
                                date DATE PRIMARY KEY,
                                open NUMERIC,
                                high NUMERIC,
                                low NUMERIC,
                                close NUMERIC,
                                volume BIGINT,
                                ticker TEXT,
                                year INTEGER,
                                month INTEGER
                            )
                        """).format(sql.Identifier(table_name))
                    )

                    insert_sql = sql.SQL("""
                        INSERT INTO {} (
                            date, open, high, low, close, volume, ticker, year, month
                        )
                        VALUES %s
                        ON CONFLICT (date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            volume = EXCLUDED.volume
                    """).format(sql.Identifier(table_name))

                    data = [
                        (
                            row["Date"],
                            row["open"],
                            row["high"],
                            row["low"],
                            row["close"],
                            row["volume"],
                            row["Ticker"],
                            row["year"],
                            row["month"],
                        )
                        for _, row in df.iterrows()
                    ]

                    execute_values.execute_values(cur, insert_sql, data)

                    print(f"Đã load {symbol} ngày {execution_date}")

    finally:
        conn.close()




with DAG(
    'vnstock_load_csv_to_postgres',
    default_args=default_args,
    description='Tải dữ liệu giá chứng khoán VN30 từ CSV vào Postgres',
    schedule='30 17 * * 1-5',  # Chạy lúc 17:30 từ T2 đến T6
    catchup=False,
    tags=['finance', 'vnstock', 'postgres'],
) as dag:
    
    # --- 2. Tác vụ: Tải dữ liệu vào Postgres (Load) ---
    load_to_postgres_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
    )