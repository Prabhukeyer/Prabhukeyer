# Created : 02/11/2022 16:12:15
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
    'job_name': 'CCFOT_P1AP_PROD_4_PRES'
}

dag = DAG(
    'CCFOT_P1AP_PROD_4_PRES',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

P_DIM_CCFOT_CONFIG = PythonOperator(
    task_id='Task_P_DIM_CCFOT_CONFIG',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_P_DIM_CCFOT_CONFIG('{task_id}', 'COMMON_LAYERS.PRESENTATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

P_DIM_CCFOT_CONFIG_MATERIAL = PythonOperator(
    task_id='Task_P_DIM_CCFOT_CONFIG_MATERIAL',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_P_DIM_CCFOT_CONFIG_MATERIAL('{task_id}', 'COMMON_LAYERS.PRESENTATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_DIM_CUSTOMER_P1AP = PythonOperator(
    task_id='Task_SRC_P_DIM_CUSTOMER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_DIM_CUSTOMER_P1AP('{task_id}', 'COMMON_LAYERS.PRESENTATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_DIM_SALES_DOCUMENT_CHANGE_REASON_P1AP = PythonOperator(
    task_id='Task_SRC_P_DIM_SALES_DOCUMENT_CHANGE_REASON_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_DIM_SALES_DOCUMENT_CHANGE_REASON_P1AP('{task_id}', 'COMMON_LAYERS.STAGE_PREPROCESSING', 'Qualtrics_preprocessing', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_DIM_SALES_DOCUMENT_REJECT_REASON_P1AP = PythonOperator(
    task_id='Task_SRC_P_DIM_SALES_DOCUMENT_REJECT_REASON_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_DIM_SALES_DOCUMENT_REJECT_REASON_P1AP('{task_id}', 'COMMON_LAYERS.STAGE_PREPROCESSING', 'Qualtrics_preprocessing', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_DIM_SALES_ORGANIZATION_P1AP = PythonOperator(
    task_id='Task_SRC_P_DIM_SALES_ORGANIZATION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_DIM_SALES_ORGANIZATION_P1AP('{task_id}', 'COMMON_LAYERS.PRESENTATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP = PythonOperator(
    task_id='Task_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP('{task_id}', 'COMMON_LAYERS.PRESENTATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_ISMS_ID = PythonOperator(
    task_id='Task_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_ISMS_ID',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_ISMS_ID('{task_id}', 'COMMON_LAYERS.PRESENTATION', '00 GNPT - PreProcessing', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP = PythonOperator(
    task_id='Task_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP('{task_id}', 'COMMON_LAYERS.PRESENTATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

SRC_STAGE_CCFOT_P1AP_DP_PROCESSED_FLAG = PythonOperator(
    task_id='Task_SRC_STAGE_CCFOT_P1AP_DP_PROCESSED_FLAG',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_SRC_STAGE_CCFOT_P1AP_DP_PROCESSED_FLAG('{task_id}', 'COMMON_LAYERS.PRESENTATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_IN = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_Common_Objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_SUM_IN = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_SUM_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_SUM_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_02_IN = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_02_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_02_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_Common_Objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_Common_Objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST_SVIEW = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST_SVIEW',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST_SVIEW('{task_id}', 'COMMON_LAYERS.PRESENTATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_SVIEW = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_SVIEW',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_SVIEW('{task_id}', 'COMMON_LAYERS.PRESENTATION', '39_CCFOT_Airflow_Common_Objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI('{task_id}', 'COMMON_LAYERS.PRESENTATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_SVIEW = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_SVIEW',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_SVIEW('{task_id}', 'COMMON_LAYERS.PRESENTATION', '39_CCFOT_Airflow_Common_Objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_SVIEW = PythonOperator(
    task_id='Task_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_SVIEW',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_SVIEW('{task_id}', 'COMMON_LAYERS.PRESENTATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_CHGLOGDT_LAST_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_CHGLOGDT_LAST_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_CHGLOGDT_LAST_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_BASE_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_BASE_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_BASE_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_LAST_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_LAST_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_LAST_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOGDT_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOGDT_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOGDT_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_DEL_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_DEL_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_DEL_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_HBASE_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_HBASE_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_HBASE_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_ORIG_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_ORIG_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_ORIG_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP_UN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP_UN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP_UN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_PO_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_PO_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_PO_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_RETURN_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_RETURN_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_RETURN_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_RECSV_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_RECSV_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_RECSV_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_REJ_P1AP_IN = PythonOperator(
    task_id='Task_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_REJ_P1AP_IN',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_REJ_P1AP_IN('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
P_DIM_CCFOT_CONFIG.set_upstream(INIT_JOB)
P_DIM_CCFOT_CONFIG_MATERIAL.set_upstream(INIT_JOB)
SRC_P_DIM_CUSTOMER_P1AP.set_upstream(INIT_JOB)
SRC_P_DIM_SALES_DOCUMENT_CHANGE_REASON_P1AP.set_upstream(INIT_JOB)
SRC_P_DIM_SALES_DOCUMENT_REJECT_REASON_P1AP.set_upstream(INIT_JOB)
SRC_P_DIM_SALES_ORGANIZATION_P1AP.set_upstream(INIT_JOB)
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_REJ_P1AP_IN.set_upstream([P_DIM_CCFOT_CONFIG,P_DIM_CCFOT_CONFIG_MATERIAL,SRC_P_DIM_CUSTOMER_P1AP,SRC_P_DIM_SALES_DOCUMENT_CHANGE_REASON_P1AP,SRC_P_DIM_SALES_DOCUMENT_REJECT_REASON_P1AP,SRC_P_DIM_SALES_ORGANIZATION_P1AP])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_DEL_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_REJ_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_DEL_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_HBASE_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DDI_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_BASE_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_HBASE_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_PO_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_BASE_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_ORIG_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_PO_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_RETURN_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_ORIG_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_RETURN_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_LAST_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOGDT_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOG_LAST_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_CHGLOGDT_LAST_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_CHGLOGDT_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_CHGLOGDT_LAST_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_RECSV_P1AP_IN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP_UN.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_SDI_RECSV_P1AP_IN])
STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP_UN])
SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP.set_upstream([STAGE_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP])
SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_ISMS_ID.set_upstream([SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_P1AP])
SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP.set_upstream([SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_ISMS_ID])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_IN.set_upstream([SRC_P_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_P1AP])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_SUM_IN.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_IN])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_02_IN.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_01_SUM_IN])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_02_IN])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_SVIEW.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_SVIEW.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_SVIEW])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST_SVIEW.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_SVIEW])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_AGG_CUST_SVIEW])
STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_SVIEW.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI])
SRC_STAGE_CCFOT_P1AP_DP_PROCESSED_FLAG.set_upstream([STAGE_DP_FACT_SALES_DOCUMENT_ITEM_DATA_DF_DDI_SVIEW])
FINISH_JOB.set_upstream([SRC_STAGE_CCFOT_P1AP_DP_PROCESSED_FLAG])

