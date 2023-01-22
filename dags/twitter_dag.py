from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl
from twitter_etl import to_s3_bucket
from twitter_etl import copy_to_redshift

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 15),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args=default_args,
    description='This dag handles fetching the data, uploading to s3 and copying the data to redshift.'
)

get_twitter_data = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl,
    dag=dag
)

upload_to_s3 = PythonOperator(
    task_id='upload data to s3',
    python_callable=to_s3_bucket,
    dag=dag
)

to_redshift = PythonOperator(
    task_id='copy data to redshift',
    python_callable=copy_to_redshift,
    dag=dag
)

get_twitter_data
upload_to_s3
to_redshift
