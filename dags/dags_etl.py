import sys
import os

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

sys.path.append("/opt/airflow/script_etl")

from oltpTrans_DWH import run_etl_oltp_dwh
from DWH_MartUserPurchase import run_etl_mart_UserPurchase
from DWH_MartUserRepurchase import run_etl_mart_UserRepurchase
from DWH_MartRevenuePeriod import run_etl_mart_RevenuePeriod
from Mart_GoogleSheet import run_mart_gsheet


with DAG(
    dag_id='etl_dwh_mart',
    start_date=datetime(2025, 4, 19),
    schedule_interval='30 3 * * *',  # Runs once a day: at 3.30 AM
    catchup=False
) as dag:
    

    start_task = EmptyOperator(
        task_id='start',
        owner='Gugus'
    )

    oltp_dwh = PythonOperator(
        task_id='oltp_dwh',
        python_callable=run_etl_oltp_dwh,
    )

    dwh_mart_user_purchase = PythonOperator(
        task_id='dwh_mart_user_purchase',
        python_callable=run_etl_mart_UserPurchase,
    )

    dwh_mart_user_repurchase = PythonOperator(
        task_id='dwh_mart_user_repurchase',
        python_callable=run_etl_mart_UserRepurchase,
    )

    dwh_mart_revenue_period = PythonOperator(
        task_id='dwh_mart_revenue_period',
        python_callable=run_etl_mart_RevenuePeriod,
    )

    mart_gsheet = PythonOperator(
        task_id='mart_gsheet',
        python_callable=run_mart_gsheet,
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> oltp_dwh >> dwh_mart_user_purchase >> dwh_mart_user_repurchase >> dwh_mart_revenue_period >> mart_gsheet >> end_task