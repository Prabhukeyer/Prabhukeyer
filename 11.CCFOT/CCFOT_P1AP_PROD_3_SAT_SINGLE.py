# Created : 02/11/2022 16:11:53
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
    'job_name': 'CCFOT_P1AP_PROD_3_SAT_SINGLE'
}

dag = DAG(
    'CCFOT_P1AP_PROD_3_SAT_SINGLE',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

S_CUSTOMER_P1AP = PythonOperator(
    task_id='Task_S_CUSTOMER_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_CUSTOMER_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_CUSTOMER_SEGMENT_P1AP = PythonOperator(
    task_id='Task_S_CUSTOMER_SEGMENT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_CUSTOMER_SEGMENT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_DELIVERY_DOCUMENT_HEADER_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_DELIVERY_DOCUMENT_HEADER_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_DELIVERY_DOCUMENT_HEADER_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_DELIVERY_DOCUMENT_ITEM_DATA_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_DELIVERY_DOCUMENT_ITEM_DATA_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_DELIVERY_DOCUMENT_ITEM_DATA_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'ZBR_x_test', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_DOMAIN_VALUE_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_DOMAIN_VALUE_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_DOMAIN_VALUE_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_MATERIAL_SALES_DATA_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_MATERIAL_SALES_DATA_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_MATERIAL_SALES_DATA_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_PURCHASING_DOCUMENT_HISTORY_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_PURCHASING_DOCUMENT_HISTORY_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_PURCHASING_DOCUMENT_HISTORY_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_CHANGELOG_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_CHANGELOG_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_CHANGELOG_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_HEADER_TRANSACT_ISMS_ID = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_HEADER_TRANSACT_ISMS_ID',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_HEADER_TRANSACT_ISMS_ID('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_HEADER_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_HEADER_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_HEADER_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'DOBW_DV_FLW3', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ITEM_CHANGELOG_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ITEM_CHANGELOG_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ITEM_CHANGELOG_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_ISMS_ID = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_ISMS_ID',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_ISMS_ID('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ITEM_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ITEM_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ITEM_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_TRANSACT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_TRANSACT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_TRANSACT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_SALES_ORGANIZATION_DESCRIPTION_P1AP = PythonOperator(
    task_id='Task_S_SALES_ORGANIZATION_DESCRIPTION_P1AP',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_SALES_ORGANIZATION_DESCRIPTION_P1AP('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_WEBAPP_CCFOT_CONFIG_MATERIAL_CATEG = PythonOperator(
    task_id='Task_S_WEBAPP_CCFOT_CONFIG_MATERIAL_CATEG',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_WEBAPP_CCFOT_CONFIG_MATERIAL_CATEG('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_WEBAPP_CCFOT_CONFIG_REASON = PythonOperator(
    task_id='Task_S_WEBAPP_CCFOT_CONFIG_REASON',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_WEBAPP_CCFOT_CONFIG_REASON('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

S_WEBAPP_CCFOT_CONFIG_SCOPE = PythonOperator(
    task_id='Task_S_WEBAPP_CCFOT_CONFIG_SCOPE',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_S_WEBAPP_CCFOT_CONFIG_SCOPE('{task_id}', 'COMMON_LAYERS.INTEGRATION', '01_DOOPS1_CCFOT', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
S_CUSTOMER_P1AP.set_upstream(INIT_JOB)
S_CUSTOMER_SEGMENT_P1AP.set_upstream(INIT_JOB)
S_DELIVERY_DOCUMENT_HEADER_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_DELIVERY_DOCUMENT_ITEM_DATA_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
S_DOMAIN_VALUE_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
S_MATERIAL_SALES_DATA_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_CHANGELOG_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_HEADER_TRANSACT_ISMS_ID.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_HEADER_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ITEM_CHANGELOG_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_ISMS_ID.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ITEM_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
S_SALES_ORGANIZATION_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
S_WEBAPP_CCFOT_CONFIG_MATERIAL_CATEG.set_upstream(INIT_JOB)
S_WEBAPP_CCFOT_CONFIG_REASON.set_upstream(INIT_JOB)
S_WEBAPP_CCFOT_CONFIG_SCOPE.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_PURCHASING_DOCUMENT_HISTORY_TRANSACT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_P1AP.set_upstream(INIT_JOB)
S_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP.set_upstream(INIT_JOB)
FINISH_JOB.set_upstream([S_CUSTOMER_P1AP,S_CUSTOMER_SEGMENT_P1AP,S_DELIVERY_DOCUMENT_HEADER_TRANSACT_P1AP,S_DELIVERY_DOCUMENT_ITEM_DATA_TRANSACT_P1AP,S_DISTRIBUTION_CHANNEL_DESCRIPTION_P1AP,S_DOMAIN_VALUE_DESCRIPTION_P1AP,S_MATERIAL_SALES_DATA_TRANSACT_P1AP,S_SALES_DOCUMENT_CHANGELOG_TRANSACT_P1AP,S_SALES_DOCUMENT_HEADER_TRANSACT_ISMS_ID,S_SALES_DOCUMENT_HEADER_TRANSACT_P1AP,S_SALES_DOCUMENT_ITEM_CHANGELOG_TRANSACT_P1AP,S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_ISMS_ID,S_SALES_DOCUMENT_ITEM_P1AP,S_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_TRANSACT_P1AP,S_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_P1AP,S_SALES_ORGANIZATION_DESCRIPTION_P1AP,S_WEBAPP_CCFOT_CONFIG_MATERIAL_CATEG,S_WEBAPP_CCFOT_CONFIG_REASON,S_WEBAPP_CCFOT_CONFIG_SCOPE,S_SALES_DOCUMENT_ITEM_DATA_TRANSACT_P1AP,S_PURCHASING_DOCUMENT_HISTORY_TRANSACT_P1AP,S_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_P1AP,S_SALES_DOCUMENT_P1AP,S_SALES_DOCUMENT_TYPE_DESCRIPTION_P1AP])

