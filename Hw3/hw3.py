from datetime import datetime, timedelta
from airflow import DAG
import logging
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import functools
import func
import task1
import task2
import task3
file="/mnt/share/egrul.json.zip"
default_args = {
    'owner': 'denisov',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

logger = logging.getLogger(__name__)
sqlite_hook = SqliteHook(sqlite_conn_id='sqlite_hw3')

with DAG(
    dag_id='HW3',
    default_args=default_args,
    start_date=datetime(2023, 7, 18),
    schedule_interval='@daily'
) as dag:
    create_tables = PythonOperator(
        task_id='create_table',
        python_callable=functools.partial(func.create_tables,sqlite_hook=sqlite_hook,logger=logger)
    )
    parse = PythonOperator(
        task_id='parse_and_insert_egrul',
        python_callable=functools.partial(task1.parse_and_insert_egrl,fl=file,sqlite_hook=sqlite_hook),
    )
    get_vac = PythonOperator(
        task_id='get_vacancy_hh',
        python_callable=functools.partial(task2.get_vacancy_hh,sqlite_hook=sqlite_hook,logger=logger),
    )
    get_top = PythonOperator(
        task_id='get_top_skills',
        python_callable=functools.partial(task3.get_top_skills,logger=logger),
                             )

create_tables>>parse>>get_vac>>get_top