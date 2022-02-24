# Created : 02/11/2022 16:10:42
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
    'job_name': 'CCFOT_P1AP_PROD_2_STAGE'
}

dag = DAG(
    'CCFOT_P1AP_PROD_2_STAGE',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

STAGE_CUSTOMER_ISMS_ID_4 = PythonOperator(
    task_id='Task_STAGE_CUSTOMER_ISMS_ID_4',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_CUSTOMER_ISMS_ID_4('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_CUSTOMER_MARKET_P1AP = PythonOperator(
    task_id='Task_STAGE_CUSTOMER_MARKET_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_CUSTOMER_MARKET_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_CUSTOMER_P1AP = PythonOperator(
    task_id='Task_STAGE_CUSTOMER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_CUSTOMER_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_CUSTOMER_SEGMENT_P1AP = PythonOperator(
    task_id='Task_STAGE_CUSTOMER_SEGMENT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_CUSTOMER_SEGMENT_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DELIVERY_DOCUMENT_HEADER_P1AP = PythonOperator(
    task_id='Task_STAGE_DELIVERY_DOCUMENT_HEADER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DELIVERY_DOCUMENT_HEADER_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', 'DOBW_DV_FLW3', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DELIVERY_DOCUMENT_ITEM_DATA_P1AP = PythonOperator(
    task_id='Task_STAGE_DELIVERY_DOCUMENT_ITEM_DATA_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DELIVERY_DOCUMENT_ITEM_DATA_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DELIVERY_DOCUMENT_DOCUMENT_ITEM_P1AP = PythonOperator(
    task_id='Task_STAGE_DELIVERY_DOCUMENT_DOCUMENT_ITEM_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DELIVERY_DOCUMENT_DOCUMENT_ITEM_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DELIVERY_DOCUMENT_ITEM_P1AP = PythonOperator(
    task_id='Task_STAGE_DELIVERY_DOCUMENT_ITEM_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DELIVERY_DOCUMENT_ITEM_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DELIVERY_DOCUMENT_P1AP = PythonOperator(
    task_id='Task_STAGE_DELIVERY_DOCUMENT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DELIVERY_DOCUMENT_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_STAGE_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DISTRIBUTION_CHANNEL_P1AP = PythonOperator(
    task_id='Task_STAGE_DISTRIBUTION_CHANNEL_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DISTRIBUTION_CHANNEL_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DOMAIN_VALUE_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_STAGE_DOMAIN_VALUE_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DOMAIN_VALUE_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_DOMAIN_VALUE_P1AP = PythonOperator(
    task_id='Task_STAGE_DOMAIN_VALUE_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_DOMAIN_VALUE_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_MATERIAL_SALES_DATA_P1AP = PythonOperator(
    task_id='Task_STAGE_MATERIAL_SALES_DATA_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_MATERIAL_SALES_DATA_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_PURCHASING_DOCUMENT_HISTORY_P1AP = PythonOperator(
    task_id='Task_STAGE_PURCHASING_DOCUMENT_HISTORY_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_PURCHASING_DOCUMENT_HISTORY_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_CHANGELOG_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_CHANGELOG_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_CHANGELOG_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_ISMS_ID_9 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_ISMS_ID_9',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_ISMS_ID_9('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP_DATA_5 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP_DATA_5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP_DATA_5('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_HEADER_ISMS_ID_4 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_HEADER_ISMS_ID_4',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_HEADER_ISMS_ID_4('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_HEADER_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_HEADER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_HEADER_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_HEADER_P1AP_HDR = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_HEADER_P1AP_HDR',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_HEADER_P1AP_HDR('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ISMS_ID_9 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ISMS_ID_9',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ISMS_ID_9('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_CHANGELOG_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_CHANGELOG_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_CHANGELOG_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_DATA_ISMS_ID_4 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_DATA_ISMS_ID_4',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_DATA_ISMS_ID_4('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP_DATA = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP_DATA',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP_DATA('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_ISMS_ID_9 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_ISMS_ID_9',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_ISMS_ID_9('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_P1AP_DATA_5 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_P1AP_DATA_5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_P1AP_DATA_5('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_PARTNER_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_PARTNER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_PARTNER_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_ORDER_REASON_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_ORDER_REASON_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_ORDER_REASON_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_P1AP_HDR_5 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_P1AP_HDR_5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_P1AP_HDR_5('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_REJECT_REASON_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_REJECT_REASON_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_REJECT_REASON_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP_2 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP_2',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP_2('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_DOCUMENT_TYPE_P1AP_2 = PythonOperator(
    task_id='Task_STAGE_SALES_DOCUMENT_TYPE_P1AP_2',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_DOCUMENT_TYPE_P1AP_2('{task_id}', 'COMMON_LAYERS.STAGING', '39_DOOPS1_CCFOT_Release_V1_26_d1_DEV_QAS', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_ORGANIZATION_COMPANY_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_ORGANIZATION_COMPANY_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_ORGANIZATION_COMPANY_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_ORGANIZATION_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_ORGANIZATION_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_ORGANIZATION_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_SALES_ORGANIZATION_P1AP = PythonOperator(
    task_id='Task_STAGE_SALES_ORGANIZATION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_SALES_ORGANIZATION_P1AP('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_WEBAPP_CCFOT_CONFIG_MATERIAL = PythonOperator(
    task_id='Task_STAGE_WEBAPP_CCFOT_CONFIG_MATERIAL',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_WEBAPP_CCFOT_CONFIG_MATERIAL('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_WEBAPP_CCFOT_CONFIG_REASON_1 = PythonOperator(
    task_id='Task_STAGE_WEBAPP_CCFOT_CONFIG_REASON_1',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_WEBAPP_CCFOT_CONFIG_REASON_1('{task_id}', 'COMMON_LAYERS.STAGING', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

STAGE_WEBAPP_CCFOT_CONFIG_SCOPE = PythonOperator(
    task_id='Task_STAGE_WEBAPP_CCFOT_CONFIG_SCOPE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_STAGE_WEBAPP_CCFOT_CONFIG_SCOPE('{task_id}', 'COMMON_LAYERS.STAGING', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
STAGE_CUSTOMER_MARKET_P1AP.set_upstream(INIT_JOB)
STAGE_CUSTOMER_P1AP.set_upstream(INIT_JOB)
STAGE_CUSTOMER_SEGMENT_P1AP.set_upstream(INIT_JOB)
STAGE_DELIVERY_DOCUMENT_DOCUMENT_ITEM_P1AP.set_upstream(INIT_JOB)
STAGE_DELIVERY_DOCUMENT_HEADER_P1AP.set_upstream(INIT_JOB)
STAGE_DELIVERY_DOCUMENT_ITEM_DATA_P1AP.set_upstream(INIT_JOB)
STAGE_DELIVERY_DOCUMENT_ITEM_P1AP.set_upstream(INIT_JOB)
STAGE_DELIVERY_DOCUMENT_P1AP.set_upstream(INIT_JOB)
STAGE_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
STAGE_DISTRIBUTION_CHANNEL_P1AP.set_upstream(INIT_JOB)
STAGE_DOMAIN_VALUE_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
STAGE_DOMAIN_VALUE_P1AP.set_upstream(INIT_JOB)
STAGE_MATERIAL_SALES_DATA_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_CHANGELOG_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP_DATA_5.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_HEADER_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_HEADER_P1AP_HDR.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_CHANGELOG_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP_DATA.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_P1AP_DATA_5.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_PARTNER_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_P1AP_HDR_5.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_REJECT_REASON_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_ORGANIZATION_COMPANY_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_ORGANIZATION_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_ORGANIZATION_P1AP.set_upstream(INIT_JOB)
STAGE_WEBAPP_CCFOT_CONFIG_MATERIAL.set_upstream(INIT_JOB)
STAGE_WEBAPP_CCFOT_CONFIG_REASON_1.set_upstream(INIT_JOB)
STAGE_WEBAPP_CCFOT_CONFIG_SCOPE.set_upstream(INIT_JOB)
STAGE_CUSTOMER_ISMS_ID_4.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_ISMS_ID_9.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_HEADER_ISMS_ID_4.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ISMS_ID_9.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_DATA_ISMS_ID_4.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ITEM_ISMS_ID_9.set_upstream(INIT_JOB)
STAGE_PURCHASING_DOCUMENT_HISTORY_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ORDER_REASON_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP_2.set_upstream(INIT_JOB)
STAGE_SALES_DOCUMENT_TYPE_P1AP_2.set_upstream(INIT_JOB)
FINISH_JOB.set_upstream([STAGE_CUSTOMER_MARKET_P1AP,STAGE_CUSTOMER_P1AP,STAGE_CUSTOMER_SEGMENT_P1AP,STAGE_DELIVERY_DOCUMENT_DOCUMENT_ITEM_P1AP,STAGE_DELIVERY_DOCUMENT_HEADER_P1AP,STAGE_DELIVERY_DOCUMENT_ITEM_DATA_P1AP,STAGE_DELIVERY_DOCUMENT_ITEM_P1AP,STAGE_DELIVERY_DOCUMENT_P1AP,STAGE_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP,STAGE_DISTRIBUTION_CHANNEL_P1AP,STAGE_DOMAIN_VALUE_DESCRIPTION_P1AP,STAGE_DOMAIN_VALUE_P1AP,STAGE_MATERIAL_SALES_DATA_P1AP,STAGE_SALES_DOCUMENT_CHANGELOG_P1AP,STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP,STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_P1AP_DATA_5,STAGE_SALES_DOCUMENT_HEADER_P1AP,STAGE_SALES_DOCUMENT_HEADER_P1AP_HDR,STAGE_SALES_DOCUMENT_ITEM_CHANGELOG_P1AP,STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP,STAGE_SALES_DOCUMENT_ITEM_DATA_P1AP_DATA,STAGE_SALES_DOCUMENT_ITEM_P1AP,STAGE_SALES_DOCUMENT_ITEM_P1AP_DATA_5,STAGE_SALES_DOCUMENT_ITEM_PARTNER_P1AP,STAGE_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_P1AP,STAGE_SALES_DOCUMENT_P1AP,STAGE_SALES_DOCUMENT_P1AP_HDR_5,STAGE_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP,STAGE_SALES_DOCUMENT_REJECT_REASON_P1AP,STAGE_SALES_ORGANIZATION_COMPANY_P1AP,STAGE_SALES_ORGANIZATION_DESCRIPTION_P1AP,STAGE_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_P1AP,STAGE_SALES_ORGANIZATION_P1AP,STAGE_WEBAPP_CCFOT_CONFIG_MATERIAL,STAGE_WEBAPP_CCFOT_CONFIG_REASON_1,STAGE_WEBAPP_CCFOT_CONFIG_SCOPE,STAGE_CUSTOMER_ISMS_ID_4,STAGE_SALES_DOCUMENT_DOCUMENT_ITEM_ISMS_ID_9,STAGE_SALES_DOCUMENT_HEADER_ISMS_ID_4,STAGE_SALES_DOCUMENT_ISMS_ID_9,STAGE_SALES_DOCUMENT_ITEM_DATA_ISMS_ID_4,STAGE_SALES_DOCUMENT_ITEM_ISMS_ID_9,STAGE_PURCHASING_DOCUMENT_HISTORY_P1AP,STAGE_SALES_DOCUMENT_ORDER_REASON_P1AP,STAGE_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP,STAGE_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP_2,STAGE_SALES_DOCUMENT_TYPE_P1AP_2])

