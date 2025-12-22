from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock
import os
import psycopg2
import psycopg2.sql as sql
import psycopg2.extras as execute_values
import airflow.hooks.base as base_hook
from airflow import DAG
from airflow.operators.python import PythonOperator



default_args = {
    'owner': 'Tuan Quang',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 16),
    'email': ['tuanquang@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

BASE_DIR = "/usr/local/airflow/dags/data_lake/vnstock_prices_csv"

airflow_conn = base_hook.BaseHook.get_connection("local_postgres")

def Extract_data_to_csv (**kwargs):
    execution_date = kwargs["ds"]
    year, month, _ = execution_date.split("-")

    list_csv_files = []
    vnstock_api = Vnstock()

    TICKERS = ['ACB', 'BVH', 'BCM', 'BID', 'FPT', 'HDB', 'HPG', 'LPB',
 'MSN', 'MBB', 'MWG', 'PLX', 'GAS', 'SAB', 'STB', 'SHB',
 'SSI', 'TCB', 'TPB', 'VCB', 'CTG', 'VJC', 'VIB', 'GVR',
 'VNM', 'VRE', 'VIC', 'VHM', 'VPB']


    for ticker in TICKERS:
        try:
            stock_obj = vnstock_api.stock(symbol=ticker, source="VCI")
            df = stock_obj.quote.history(
                start=f"{year}-01-01",
                end=execution_date,
                interval="1D"
            )

            if not df.empty:
# vnstock trả ngày ở cột 'time'
                df.rename(columns={'time': 'Date'}, inplace=True)

                # đảm bảo Date đúng kiểu
                df['Date'] = pd.to_datetime(df['Date']).dt.date

                df['Ticker'] = ticker
                df['year'] = pd.to_datetime(df['Date']).dt.year
                df['month'] = pd.to_datetime(df['Date']).dt.month

                df.rename(columns={
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'volume': 'Volume'
                }, inplace=True)


                output_dir = os.path.join(BASE_DIR, f"year={year}")
                os.makedirs(output_dir, exist_ok=True)
                output_file = os.path.join(output_dir, f"{ticker}_execution_date_{execution_date}.csv")
                df.to_csv(output_file, index=False)
                list_csv_files.append(output_file)
            else:
                print(f"No data for {ticker} on {execution_date}")

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
    return list_csv_files

def create_table_if_not_exists(**kwargs):
    ti = kwargs['ti']
    list_files = ti.xcom_pull(task_ids='extract_data_to_csv')

    conn = psycopg2.connect(
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port
    )
    cursor = conn.cursor()

    for file_path in list_files:
        file_name = os.path.basename(file_path)
        file_no_ext = os.path.splitext(file_name)[0]

        ticker = file_no_ext.split("_")[0].lower()
        table_name = f"vnstock_{ticker}"

        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                Date DATE,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume BIGINT,
                year INT,
                month INT,
                PRIMARY KEY (Date)
            );
        """)

    conn.commit()
    cursor.close()
    conn.close()

def load_csv_to_postgres(**kwargs):
    conn = psycopg2.connect(
        dbname=airflow_conn.schema,
        user=airflow_conn.login,
        password=airflow_conn.password,
        host=airflow_conn.host,
        port=airflow_conn.port
    )
    cursor = conn.cursor()

    execution_date = kwargs["ds"]
    year, _, _ = execution_date.split("-")

    ti = kwargs['ti']
    list_files = ti.xcom_pull(task_ids='extract_data_to_csv')

    for file_path in list_files:
        file_name = os.path.basename(file_path)
        file_no_ext = os.path.splitext(file_name)[0]

        ticker = file_no_ext.split("_")[0].lower()
        table_name = f"vnstock_{ticker}"

        df = pd.read_csv(file_path)

        records = df.to_dict(orient='records')

        insert_query = sql.SQL(f"""
            INSERT INTO {table_name}
            (Date, Open, High, Low, Close, Volume, year, month)
            VALUES %s
            ON CONFLICT (Date) DO NOTHING
        """)

        values = [
            (
                r['Date'],
                r['Open'],
                r['High'],
                r['Low'],
                r['Close'],
                r['Volume'],
                r['year'],
                r['month']
            )
            for r in records
        ]

        execute_values.execute_values(
            cursor,
            insert_query.as_string(conn),
            values
        )

        conn.commit()

    cursor.close()
    conn.close()


with DAG(
    'vnstock_etl_dag',
    default_args=default_args,
    description='ETL DAG for Vnstock data to Postgres',
    schedule='@yearly',
    catchup=True,
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data_to_csv',
        python_callable=Extract_data_to_csv,
    )

    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists,
    )

    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    extract_task >> create_table_task >> load_task
