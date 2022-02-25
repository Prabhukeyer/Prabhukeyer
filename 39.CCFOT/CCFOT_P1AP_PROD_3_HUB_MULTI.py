# Created : 02/11/2022 16:10:58
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
    'job_name': 'CCFOT_P1AP_PROD_3_HUB_MULTI'
}

dag = DAG(
    'CCFOT_P1AP_PROD_3_HUB_MULTI',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

src_H_CUSTOMER_3836ba25 = PythonOperator(
    task_id='Task_src_H_CUSTOMER_3836ba25',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_CUSTOMER_3836ba25('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_CUSTOMER_4_570d09a = PythonOperator(
    task_id='Task_src_H_CUSTOMER_4_570d09a',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_CUSTOMER_4_570d09a('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_DELIVERY_DOCUMENT_6021f805 = PythonOperator(
    task_id='Task_src_H_DELIVERY_DOCUMENT_6021f805',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_DELIVERY_DOCUMENT_6021f805('{task_id}', 'ZBR.IM', 'webapp_dp', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_DELIVERY_DOCUMENT_ITEM_7e0bdec1 = PythonOperator(
    task_id='Task_src_H_DELIVERY_DOCUMENT_ITEM_7e0bdec1',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_DELIVERY_DOCUMENT_ITEM_7e0bdec1('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_DISTRIBUTION_CHANNEL_fd730eb9 = PythonOperator(
    task_id='Task_src_H_DISTRIBUTION_CHANNEL_fd730eb9',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_DISTRIBUTION_CHANNEL_fd730eb9('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_DOMAIN_VALUE_cae27395 = PythonOperator(
    task_id='Task_src_H_DOMAIN_VALUE_cae27395',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_DOMAIN_VALUE_cae27395('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_5_8b4b5c7b = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_5_8b4b5c7b',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_5_8b4b5c7b('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_9_8b4b5c7f = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_9_8b4b5c7f',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_9_8b4b5c7f('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_c102be05 = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_c102be05',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_c102be05('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_ITEM_22e897c5 = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_ITEM_22e897c5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_ITEM_22e897c5('{task_id}', 'COMMON_LAYERS.STAGING', '03 GNPT - SAP entities', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_ITEM_5_b21c63b = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_ITEM_5_b21c63b',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_ITEM_5_b21c63b('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_ITEM_9_b21c63f = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_ITEM_9_b21c63f',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_ITEM_9_b21c63f('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'ZBR_COST_MERGE_all_objects', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_ORDER_REASON_9b173485 = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_ORDER_REASON_9b173485',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_ORDER_REASON_9b173485('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_REJECT_REASON_e6fd67cf = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_REJECT_REASON_e6fd67cf',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_REJECT_REASON_e6fd67cf('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_DOCUMENT_TYPE_2_1003fdd8 = PythonOperator(
    task_id='Task_src_H_SALES_DOCUMENT_TYPE_2_1003fdd8',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_DOCUMENT_TYPE_2_1003fdd8('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_SALES_ORGANIZATION_3a54fe35 = PythonOperator(
    task_id='Task_src_H_SALES_ORGANIZATION_3a54fe35',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_SALES_ORGANIZATION_3a54fe35('{task_id}', 'ZBR.IM', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7 = PythonOperator(
    task_id='Task_src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_H_WEBAPP_CCFOT_CONFIG_7bf03d45 = PythonOperator(
    task_id='Task_src_H_WEBAPP_CCFOT_CONFIG_7bf03d45',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_H_WEBAPP_CCFOT_CONFIG_7bf03d45('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

H_WEBAPP_CCFOT_CONFIG_MATERIAL = PythonOperator(
    task_id='Task_H_WEBAPP_CCFOT_CONFIG_MATERIAL',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_H_WEBAPP_CCFOT_CONFIG_MATERIAL('{task_id}', 'COMMON_LAYERS.INTEGRATION', '39_CCFOT_Airflow_P1AP', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
src_H_DELIVERY_DOCUMENT_6021f805.set_upstream(INIT_JOB)
src_H_DELIVERY_DOCUMENT_ITEM_7e0bdec1.set_upstream(INIT_JOB)
src_H_DISTRIBUTION_CHANNEL_fd730eb9.set_upstream(INIT_JOB)
src_H_DOMAIN_VALUE_cae27395.set_upstream(INIT_JOB)
src_H_SALES_ORGANIZATION_3a54fe35.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_5_8b4b5c7b.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_ITEM_22e897c5.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_ORDER_REASON_9b173485.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_REJECT_REASON_e6fd67cf.set_upstream(INIT_JOB)
src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7.set_upstream(INIT_JOB)
src_H_CUSTOMER_3836ba25.set_upstream(INIT_JOB)
H_WEBAPP_CCFOT_CONFIG_MATERIAL.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_TYPE_2_1003fdd8.set_upstream(INIT_JOB)
src_H_SALES_DOCUMENT_9_8b4b5c7f.set_upstream([src_H_SALES_DOCUMENT_5_8b4b5c7b])
src_H_SALES_DOCUMENT_c102be05.set_upstream([src_H_SALES_DOCUMENT_9_8b4b5c7f])
src_H_SALES_DOCUMENT_ITEM_5_b21c63b.set_upstream([src_H_SALES_DOCUMENT_ITEM_22e897c5])
src_H_SALES_DOCUMENT_ITEM_9_b21c63f.set_upstream([src_H_SALES_DOCUMENT_ITEM_5_b21c63b])
src_H_WEBAPP_CCFOT_CONFIG_7bf03d45.set_upstream([src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7])
src_H_CUSTOMER_4_570d09a.set_upstream([src_H_CUSTOMER_3836ba25])
FINISH_JOB.set_upstream([src_H_DELIVERY_DOCUMENT_6021f805,	src_H_DELIVERY_DOCUMENT_ITEM_7e0bdec1,	src_H_DISTRIBUTION_CHANNEL_fd730eb9,	src_H_DOMAIN_VALUE_cae27395,	src_H_SALES_ORGANIZATION_3a54fe35,	src_H_SALES_DOCUMENT_5_8b4b5c7b,	src_H_SALES_DOCUMENT_ITEM_22e897c5,	src_H_SALES_DOCUMENT_ORDER_REASON_9b173485,	src_H_SALES_DOCUMENT_REJECT_REASON_e6fd67cf,	src_H_WEBAPP_CCFOT_CONFIG_1_40d60bb7,	src_H_CUSTOMER_3836ba25,	H_WEBAPP_CCFOT_CONFIG_MATERIAL,	src_H_SALES_DOCUMENT_TYPE_2_1003fdd8,	src_H_SALES_DOCUMENT_9_8b4b5c7f,	src_H_SALES_DOCUMENT_c102be05,	src_H_SALES_DOCUMENT_ITEM_5_b21c63b,	src_H_SALES_DOCUMENT_ITEM_9_b21c63f,	src_H_WEBAPP_CCFOT_CONFIG_7bf03d45,	src_H_CUSTOMER_4_570d09a])
