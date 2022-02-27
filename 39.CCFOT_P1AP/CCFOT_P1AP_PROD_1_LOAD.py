# Created : 02/11/2022 16:10:14
# Start job : 12/14/2021 00:05:00

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
    'job_name': 'CCFOT_P1AP_PROD_1_LOAD'
}

dag = DAG(
    'CCFOT_P1AP_PROD_1_LOAD',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

LOAD_ISMSID_FILLRATE = PythonOperator(
    task_id='Task_LOAD_ISMSID_FILLRATE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_ISMSID_FILLRATE('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_DD07V = PythonOperator(
    task_id='Task_LOAD_P1AP_DD07V',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_DD07V('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_KNA1 = PythonOperator(
    task_id='Task_LOAD_P1AP_KNA1',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_KNA1('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_LIKP = PythonOperator(
    task_id='Task_LOAD_P1AP_LIKP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_LIKP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_EKBE = PythonOperator(
    task_id='Task_LOAD_P1AP_EKBE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_EKBE('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_LIPS = PythonOperator(
    task_id='Task_LOAD_P1AP_LIPS',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_LIPS('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_MVKE = PythonOperator(
    task_id='Task_LOAD_P1AP_MVKE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_MVKE('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVAG = PythonOperator(
    task_id='Task_LOAD_P1AP_TVAG',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVAG('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVAKT = PythonOperator(
    task_id='Task_LOAD_P1AP_TVAKT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVAKT('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVAGT = PythonOperator(
    task_id='Task_LOAD_P1AP_TVAGT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVAGT('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVAUT = PythonOperator(
    task_id='Task_LOAD_P1AP_TVAUT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVAUT('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVKO = PythonOperator(
    task_id='Task_LOAD_P1AP_TVKO',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVKO('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVKOT = PythonOperator(
    task_id='Task_LOAD_P1AP_TVKOT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVKOT('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVKOV = PythonOperator(
    task_id='Task_LOAD_P1AP_TVKOV',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVKOV('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVTW = PythonOperator(
    task_id='Task_LOAD_P1AP_TVTW',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVTW('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_TVTWT = PythonOperator(
    task_id='Task_LOAD_P1AP_TVTWT',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_TVTWT('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBAK = PythonOperator(
    task_id='Task_LOAD_P1AP_VBAK',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBAK('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBAP = PythonOperator(
    task_id='Task_LOAD_P1AP_VBAP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBAP('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBEP = PythonOperator(
    task_id='Task_LOAD_P1AP_VBEP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBEP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBPA = PythonOperator(
    task_id='Task_LOAD_P1AP_VBPA',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBPA('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBUK = PythonOperator(
    task_id='Task_LOAD_P1AP_VBUK',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBUK('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_VBUP = PythonOperator(
    task_id='Task_LOAD_P1AP_VBUP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_VBUP('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_P1AP_ZGWXS_OTIF = PythonOperator(
    task_id='Task_LOAD_P1AP_ZGWXS_OTIF',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_P1AP_ZGWXS_OTIF('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_WEBAPP_CCFOT_CONFIG_MATERIAL = PythonOperator(
    task_id='Task_LOAD_WEBAPP_CCFOT_CONFIG_MATERIAL',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_WEBAPP_CCFOT_CONFIG_MATERIAL('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_WEBAPP_CCFOT_CONFIG_REASON = PythonOperator(
    task_id='Task_LOAD_WEBAPP_CCFOT_CONFIG_REASON',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_WEBAPP_CCFOT_CONFIG_REASON('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_WEBAPP_CCFOT_CONFIG_SCOPE = PythonOperator(
    task_id='Task_LOAD_WEBAPP_CCFOT_CONFIG_SCOPE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_WEBAPP_CCFOT_CONFIG_SCOPE('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

LOAD_WEBAPP_CCFOT_CUST_SEGMENT_AP = PythonOperator(
    task_id='Task_LOAD_WEBAPP_CCFOT_CUST_SEGMENT_AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_LOAD_WEBAPP_CCFOT_CUST_SEGMENT_AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
LOAD_P1AP_DD07V.set_upstream(INIT_JOB)
LOAD_P1AP_KNA1.set_upstream(INIT_JOB)
LOAD_P1AP_LIKP.set_upstream(INIT_JOB)
LOAD_P1AP_LIPS.set_upstream(INIT_JOB)
LOAD_P1AP_MVKE.set_upstream(INIT_JOB)
LOAD_P1AP_TVAG.set_upstream(INIT_JOB)
LOAD_P1AP_TVAGT.set_upstream(INIT_JOB)
LOAD_P1AP_TVKO.set_upstream(INIT_JOB)
LOAD_P1AP_TVKOT.set_upstream(INIT_JOB)
LOAD_P1AP_TVKOV.set_upstream(INIT_JOB)
LOAD_P1AP_TVTW.set_upstream(INIT_JOB)
LOAD_P1AP_TVTWT.set_upstream(INIT_JOB)
LOAD_P1AP_VBAK.set_upstream(INIT_JOB)
LOAD_P1AP_VBAP.set_upstream(INIT_JOB)
LOAD_P1AP_VBEP.set_upstream(INIT_JOB)
LOAD_P1AP_VBPA.set_upstream(INIT_JOB)
LOAD_P1AP_VBUK.set_upstream(INIT_JOB)
LOAD_P1AP_VBUP.set_upstream(INIT_JOB)
LOAD_P1AP_ZGWXS_OTIF.set_upstream(INIT_JOB)
LOAD_WEBAPP_CCFOT_CUST_SEGMENT_AP.set_upstream(INIT_JOB)
LOAD_WEBAPP_CCFOT_CONFIG_MATERIAL.set_upstream(INIT_JOB)
LOAD_WEBAPP_CCFOT_CONFIG_REASON.set_upstream(INIT_JOB)
LOAD_WEBAPP_CCFOT_CONFIG_SCOPE.set_upstream(INIT_JOB)
LOAD_ISMSID_FILLRATE.set_upstream(INIT_JOB)
LOAD_P1AP_EKBE.set_upstream(INIT_JOB)
LOAD_P1AP_TVAUT.set_upstream(INIT_JOB)
LOAD_P1AP_TVAKT.set_upstream(INIT_JOB)
FINISH_JOB.set_upstream([LOAD_P1AP_DD07V,LOAD_P1AP_KNA1,LOAD_P1AP_LIKP,LOAD_P1AP_LIPS,LOAD_P1AP_MVKE,LOAD_P1AP_TVAG,LOAD_P1AP_TVAGT,LOAD_P1AP_TVKO,LOAD_P1AP_TVKOT,LOAD_P1AP_TVKOV,LOAD_P1AP_TVTW,LOAD_P1AP_TVTWT,LOAD_P1AP_VBAK,LOAD_P1AP_VBAP,LOAD_P1AP_VBEP,LOAD_P1AP_VBPA,LOAD_P1AP_VBUK,LOAD_P1AP_VBUP,LOAD_P1AP_ZGWXS_OTIF,LOAD_WEBAPP_CCFOT_CUST_SEGMENT_AP,LOAD_WEBAPP_CCFOT_CONFIG_MATERIAL,LOAD_WEBAPP_CCFOT_CONFIG_REASON,LOAD_WEBAPP_CCFOT_CONFIG_SCOPE,LOAD_ISMSID_FILLRATE,LOAD_P1AP_EKBE,LOAD_P1AP_TVAUT,LOAD_P1AP_TVAKT])

