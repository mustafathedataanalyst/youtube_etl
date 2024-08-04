from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
sys.path.append(r'N:\Projects\Youtube_ETL_Project\Youtube_ETL.py')
from Youtube_ETL import run_youtube_etl


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024,8, 2),
    'email' : ['cannabisradarforbusinesss@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'youtube_dag',
    default_args = default_args,
    description=''
)

run_etl = PythonOperator(
    task_id='complete_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag,
)