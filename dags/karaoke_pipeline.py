from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

# Define timezone (UTC+7)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'karaoke_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 27, tzinfo=local_tz),  # timezone-aware
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='karaoke_pipeline',
    default_args=default_args,
    description='Automated orchestration for Karaoke ETL',
    # schedule_interval=None,  # Temporarily disabled schedule so you can trigger it manually!
    schedule_interval='0 8,10,16 * * *',  # 08:00, 10:00, 16:00 (UTC+7)
    catchup=False,
    tags=['karaoke', 'etl'],
) as dag:

    # Step 1: Ingestion (SQL Server -> BigQuery)
    ingest = BashOperator(
        task_id='ingestion_pyodbc',
        bash_command='cd /opt/airflow/karaoke/ingestion && python el.py',
    )

    # Step 2: Transformation (dbt)
    transform = BashOperator(
        task_id='transformation_dbt',
        bash_command='cd /opt/airflow/karaoke/warehouse && dbt run --profiles-dir /opt/airflow/karaoke/warehouse',
    )

    # Task dependency
    ingest >> transform