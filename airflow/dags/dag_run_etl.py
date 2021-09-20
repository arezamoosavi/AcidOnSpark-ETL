from __future__ import print_function

from datetime import datetime

import airflow
from airflow.operators.bash_operator import BashOperator

args = {
    "owner": "airflow",
    "provide_context": True,
    "catchup": False,
}

dag = airflow.DAG(
    dag_id="daily_etl",
    default_args=args,
    start_date=datetime(year=2021, month=9, day=19),
    schedule_interval="0 07 * * *",
    max_active_runs=1,
    concurrency=1,
)

start_task = BashOperator(
    task_id="start_task",
    bash_command="echo daily ETL for today_date: {{ ds }}",
    dag=dag,
)

task_spark_etl = BashOperator(
    task_id="spark_etl",
    bash_command="spark-submit "
    "--master local "
    "--deploy-mode client "
    "--driver-memory 2g --conf spark.network.timeout=10000s "
    "--jars {{var.value.airflow_home}}/dags/catfish_dwh/jars/postgresql-42.2.5.jar "
    "--py-files {{var.value.airflow_home}}/dags/catfish_dwh/utils/connector.py,"
    "{{var.value.airflow_home}}/dags/utils/common.py,"
    "{{var.value.airflow_home}}/dags/etl/spark_app.py ",
    dag=dag,
)

start_task >> task_spark_etl
