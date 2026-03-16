from datetime import datetime, timedelta
import pandas as pd
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BASE_DIR = "/usr/local/airflow/dags/data_lake/weather_hcm"

def extract_weather_data(**kwargs):
    current_time = datetime.now()
    weather_data = {
        'datetime': current_time.strftime('%Y-%m-%d %H:%M:%S'),
        'city': 'Ho Chi Minh City',
        'country': 'Vietnam',
        'temperature_celsius': 30.5,  # Mock temperature
        'humidity_percent': 75,       # Mock humidity
        'weather_description': 'Partly cloudy',
        'wind_speed_kmh': 15.2        # Mock wind speed
    }
    return weather_data

def transform_weather_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='extract_weather')

    # Simple transformation: add a processed timestamp
    raw_data['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Convert to DataFrame for easier handling
    df = pd.DataFrame([raw_data])
    return df

def load_weather_to_csv(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='transform_weather')

    # Ensure output directory exists
    os.makedirs(BASE_DIR, exist_ok=True)

    # Save to CSV with timestamp in filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(BASE_DIR, f'weather_hcm_{timestamp}.csv')
    df.to_csv(output_file, index=False)

    print(f"Weather data saved to {output_file}")

with DAG(
    'weather_hcm_etl',
    default_args=default_args,
    description='Basic ETL for weather data in Ho Chi Minh City',
    schedule='@once',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'etl', 'hcm'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_data,
    )

    transform_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather_data,
    )

    load_task = PythonOperator(
        task_id='load_weather_to_csv',
        python_callable=load_weather_to_csv,
    )

    extract_task >> transform_task >> load_task
