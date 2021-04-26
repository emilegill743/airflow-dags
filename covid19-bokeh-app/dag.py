from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from covid19_bokeh_app_etl_utils import jhu_cases_etl


default_args = {
    'owner': 'emilegill743',
    'email': 'emilegill743@hotmail.com',
    'email_on_failure': True,
    'start_date': datetime(2021, 4, 23),
    'retries': 3
}

postgres_hook = PostgresHook("postgres_rds_conn_covid_19")
connection_uri = postgres_hook.get_uri()

etl_dag = DAG(
    dag_id='covid_19_bokeh_app_etl',
    default_args=default_args
)

extract_jhu_cases_task = PythonOperator(
                            task_id='extract_jhu_cases_task',
                            python_callable=jhu_cases_etl,
                            op_kwargs={"connection_uri": connection_uri},
                            dag=etl_dag)

