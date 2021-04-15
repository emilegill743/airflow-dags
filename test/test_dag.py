from airflow.models import DAG
from airflow.operators.email_operator import EmailOperator

from datetime import datetime

default_arguments = {
    'owner': 'emilegill743',
    'email': 'emilegill743@hotmail.com',
    'start_date': datetime(2021, 4, 14),
    'schedule_interval': '@daily',
    'retries': 3
}

test_email_dag = DAG(
    dag_id='test_email_dag',
    default_args=default_args
)

email_task = EmailOperator(
    task_id='test_email',
    to='emilegill743@hotmail.com',
    subject='Airflow Test',
    html_content='<title>This is a Test</title>',
    dag=test_email_dag
)

