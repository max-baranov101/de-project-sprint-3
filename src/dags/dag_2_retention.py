import time
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
       'customer_retention',
        default_args=args,
        description='Calculate customer retention mart.f_customer_retention',
        catchup=True,
        start_date=datetime.today() - timedelta(days=1),
        schedule_interval = "30 8 * * MON"
) as dag:

    del_old_retention = PostgresOperator(
        task_id='del_old_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/2_2_del_old_retention.sql")

    calc_weekly_retention = PostgresOperator(
        task_id='calc_weekly_retention',
        postgres_conn_id=postgres_conn_id,
        sql="sql/2_3_calc_weekly_retention.sql")

(
    del_old_retention >> calc_weekly_retention
)