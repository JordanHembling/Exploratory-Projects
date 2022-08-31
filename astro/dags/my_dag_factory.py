from email.policy import default
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.decorators import task, dag
from airflow.operators.subdag_operator import SubDagOperator

from datetime import datetime, timedelta
from typing import Dict
from subdag.subdag_factory import subdag_factory

@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2021, 1,1)
}

class CustomPostgresOperator(PostgresOperator):

    template_fields = ('sql', 'parameters')
    
@dag(description="DAG in charge of processing customer data",
    default_args=default_args,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=10),
    tags=["data_science", "customers"],
    catchup=False,
    max_active_runs=1)

def my_dag_factory():

    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory("my_dag_factory", "process_tasks", default_args)
    )

    extract() >> process_tasks

my_dag_factory = my_dag_factory()