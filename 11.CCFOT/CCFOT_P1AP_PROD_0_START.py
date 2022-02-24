# Created : 02/11/2022 16:09:56
# Start job : 09/25/2021 00:01:00

from airflow.models import DAG, Variable
                                        
from helpers.job_executor import JobExecutor
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

CRON = None

dag_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

sp_default_args = {
    'create': 'true',
    'execution_user_name': 'AIRFLOW', 
    'resource_group': 'ETL_XS', 
    'restarted': 'false',
    'job_name': 'CCFOT_P1AP_PROD_0_START'
}

dag = DAG(
    'CCFOT_P1AP_PROD_0_START',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

LOAD_P1AP_CONTROL_FILE_CCFOT = PythonOperator(
    task_id='Task_LOAD_P1AP_CONTROL_FILE_CCFOT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_CONTROL_FILE_CCFOT('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
LOAD_P1AP_CONTROL_FILE_CCFOT.set_upstream(INIT_JOB)
FINISH_JOB.set_upstream([LOAD_P1AP_CONTROL_FILE_CCFOT])

