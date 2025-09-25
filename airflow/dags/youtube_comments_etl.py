from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_dags = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 22),
    'email_on_failure': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    dag_id='youtube_comments_etl',
    default_args=default_dags,
    description='ETL YouTube Comments to BigQuery',
    catchup=False
) as dag:
    
    run_scripts = BashOperator(
        task_id='run_youtube_comments_etl',
        bash_command='python /opt/airflow/scripts/main.py',
        env={'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/config/gcp_key.json'}
    )

    run_scripts
