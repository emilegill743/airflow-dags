from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator)

from datetime import datetime
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from covid19_bokeh_app_etl_utils import (
    jhu_cases_etl, jhu_deaths_etl, jhu_lookup_etl,
    jhu_us_cases_etl, jhu_us_deaths_etl,
    local_uk_data_etl, owid_global_vaccinations_etl,
    bloomberg_global_vaccinations_etl,
    load_to_s3)


default_args = {
    'owner': 'emilegill743',
    'email': 'emilegill743@hotmail.com',
    'email_on_failure': True,
    'start_date': datetime(2021, 4, 23),
    'retries': 3,
    'catchup': False,
    'max_active_runs': 1,
    'dir': '/home/emilegill743/Projects/dbt_models/covid19_bokeh_app',
    'profiles_dir': '/home/emilegill743/Projects/dbt_models',
    'dbt_bin': '/home/emilegill743/Projects/airflow_venv/venv/bin'
}

postgres_hook = PostgresHook("postgres_rds_conn_covid_19")
connection_uri = postgres_hook.get_uri()

s3_storage_options = {
    'key': Variable.get('AWS_ACCESS_KEY_ID'),
    'secret': Variable.get('AWS_SECRET_ACCESS_KEY'),
    's3_additional_kwargs': {'ACL': 'public-read'}}

dbt_vars = {
    'DBT_USER': Variable.get('DBT_USER'),
    'DBT_PASSWORD':  Variable.get('DBT_PASSWORD')
    }

with DAG(dag_id='covid_19_bokeh_app_etl',
         default_args=default_args,
         schedule_interval="0 */3 * * *") as dag:

    dbt_seed = DbtSeedOperator(task_id='dbt_seed',
                               vars=dbt_vars)

    dbt_run = DbtRunOperator(task_id='dbt_run',
                             vars=dbt_vars)

    dbt_test = DbtTestOperator(task_id='dbt_test',
                               vars=dbt_vars,
                               retries=0)

    dbt_seed >> dbt_run >> dbt_test

    extract_jobs = {
        'jhu_cases': jhu_cases_etl,
        'jhu_deaths': jhu_deaths_etl,
        'jhu_lookup': jhu_lookup_etl,
        'jhu_us_cases': jhu_us_cases_etl,
        'jhu_us_deaths': jhu_us_deaths_etl,
        'local_uk_data': local_uk_data_etl,
        'owid_global_vaccinations': owid_global_vaccinations_etl,
        'bloomberg_global_vaccinations': bloomberg_global_vaccinations_etl
        }

    for job_name in extract_jobs.keys():

        extract_task = PythonOperator(
                task_id=f'extract_{job_name}',
                python_callable=extract_jobs[job_name],
                op_kwargs={"connection_uri": connection_uri})
        
        extract_task >> dbt_seed

    data_views = [
        'country_trajectories',
        'geo_time_evolution',
        'global_by_day',
        'continents_by_day',
        'local_uk',
        'vaccinations_by_country_by_day',
        'vaccinations_by_continent_by_day'
        ]

    for view in data_views:

        load_task = PythonOperator(
            task_id=f'load_{view}',
            python_callable=load_to_s3,
            op_kwargs={
                "table_name": view,
                "connection_uri": connection_uri,
                "s3_storage_options": s3_storage_options},
            provide_context=True)
        
        load_task << dbt_test
