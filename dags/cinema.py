import os
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from batch import run

schedule = os.getenv("DAG_SCHEDULE", "0 0 * * *")
if schedule == "manual":
    schedule = None

with DAG(
    dag_id="cinema",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=schedule,
    catchup=False,
    max_active_runs=1
) as dag:
    prepare = PythonOperator(task_id="prepare", python_callable=run, op_kwargs={"stage": "prepare"}, retries=3, retry_delay=timedelta(seconds=15))
    scd2 = PythonOperator(task_id="scd2", python_callable=run, op_kwargs={"stage": "scd2"})
    join = PythonOperator(task_id="join", python_callable=run, op_kwargs={"stage": "join"})
    marts = PythonOperator(task_id="marts", python_callable=run, op_kwargs={"stage": "marts"})
    checks = PythonOperator(task_id="checks", python_callable=run, op_kwargs={"stage": "checks"})
    publish = PythonOperator(task_id="publish", python_callable=run, op_kwargs={"stage": "publish"})
    prepare >> scd2 >> join >> marts >> checks >> publish
