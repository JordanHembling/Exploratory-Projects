from airflow.models import DAG 
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from datetime import datetime, timedelta 


default_args = {
    "owner": "airflow",
    "start_date": datetime(2020,1,1)
    }

with DAG(dag_id="cleaning_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False) as dag:

    # waiting_for_task = ExternalTaskSensor(
    #     task_id="waiting_for_task",
    #     external_dag_id="my_dag",
    #     external_task_id="storing",
    #     failed_states=['failed', 'skipped'],
    #     allowed_states=['success']
    # )

    cleaning_xcoms = PostgresOperator(
        task_id="cleaning_xcoms",
        sql="sql/CLEANING_XCOMS.sql",
        postgres_conn_id="postgres"
    )

    cleaning_xcoms