# Created : 02/11/2022 16:11:30
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
    'job_name': 'CCFOT_P1AP_PROD_3_LINK_MULTI'
}

dag = DAG(
    'CCFOT_P1AP_PROD_3_LINK_MULTI',
    description='Custom Operator dag',
    default_args=dag_default_args,
    start_date=days_ago(1),
    schedule_interval=CRON,
    max_active_tasks=8,
    catchup=False,
    tags=['CCFOT_P1AP']
)

INIT_JOB = DummyOperator(task_id='INIT_JOB')

src_L_MATERIAL_SALES_DATA_d52342a5 = PythonOperator(
    task_id='Task_src_L_MATERIAL_SALES_DATA_d52342a5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_MATERIAL_SALES_DATA_d52342a5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_PURCHASING_DOCUMENT_HISTORY_470b3625 = PythonOperator(
    task_id='Task_src_L_PURCHASING_DOCUMENT_HISTORY_470b3625',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_PURCHASING_DOCUMENT_HISTORY_470b3625('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_CHANGELOG_a1b38e67 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_CHANGELOG_a1b38e67',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_CHANGELOG_a1b38e67('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63('{task_id}', 'COMMON_LAYERS.STAGING', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_DOCUMENT_ITEM_9_abf41f67 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_9_abf41f67',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_DOCUMENT_ITEM_9_abf41f67('{task_id}', 'COMMON_LAYERS.INTEGRATION', '20_DOOPS4_PP_ALL', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_HEADER_4_aae58fa = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_HEADER_4_aae58fa',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_HEADER_4_aae58fa('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_HEADER_9dfada85 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_HEADER_9dfada85',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_HEADER_9dfada85('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ITEM_CHANGELOG_47908965 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ITEM_CHANGELOG_47908965',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ITEM_CHANGELOG_47908965('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ITEM_DATA_4_deaf7320 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ITEM_DATA_4_deaf7320',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ITEM_DATA_4_deaf7320('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ITEM_PARTNER_9cd780a5 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ITEM_PARTNER_9cd780a5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ITEM_PARTNER_9cd780a5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_6e417ee5 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_6e417ee5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_6e417ee5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_ccec2165 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_ccec2165',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_ccec2165('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_763bc981 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_763bc981',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_763bc981('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_DOCUMENT_TYPE_DESCRIPTION_2_a8a5e138 = PythonOperator(
    task_id='Task_src_L_SALES_DOCUMENT_TYPE_DESCRIPTION_2_a8a5e138',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_DOCUMENT_TYPE_DESCRIPTION_2_a8a5e138('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_ORGANIZATION_COMPANY_54fedee9 = PythonOperator(
    task_id='Task_src_L_SALES_ORGANIZATION_COMPANY_54fedee9',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_ORGANIZATION_COMPANY_54fedee9('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_ORGANIZATION_DESCRIPTION_f50913a7 = PythonOperator(
    task_id='Task_src_L_SALES_ORGANIZATION_DESCRIPTION_f50913a7',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_ORGANIZATION_DESCRIPTION_f50913a7('{task_id}', 'COMMON_LAYERS.INTEGRATION', '20_DOOPS4_PP_ALL', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_90815da5 = PythonOperator(
    task_id='Task_src_L_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_90815da5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_90815da5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_CUSTOMER_MARKET_f837c325 = PythonOperator(
    task_id='Task_src_L_CUSTOMER_MARKET_f837c325',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_CUSTOMER_MARKET_f837c325('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_CUSTOMER_SEGMENT_44e44c5 = PythonOperator(
    task_id='Task_src_L_CUSTOMER_SEGMENT_44e44c5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_CUSTOMER_SEGMENT_44e44c5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_DELIVERY_DOCUMENT_DOCUMENT_ITEM_8a52ce05 = PythonOperator(
    task_id='Task_src_L_DELIVERY_DOCUMENT_DOCUMENT_ITEM_8a52ce05',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_DELIVERY_DOCUMENT_DOCUMENT_ITEM_8a52ce05('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_DELIVERY_DOCUMENT_HEADER_3eff372d = PythonOperator(
    task_id='Task_src_L_DELIVERY_DOCUMENT_HEADER_3eff372d',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_DELIVERY_DOCUMENT_HEADER_3eff372d('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_DELIVERY_DOCUMENT_ITEM_DATA_4fb91ea5 = PythonOperator(
    task_id='Task_src_L_DELIVERY_DOCUMENT_ITEM_DATA_4fb91ea5',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_DELIVERY_DOCUMENT_ITEM_DATA_4fb91ea5('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_DISTRIBUTION_CHANNEL_DESCRIPTION_89b41aeb = PythonOperator(
    task_id='Task_src_L_DISTRIBUTION_CHANNEL_DESCRIPTION_89b41aeb',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_DISTRIBUTION_CHANNEL_DESCRIPTION_89b41aeb('{task_id}', 'COMMON_LAYERS.INTEGRATION', 'MULTISOURCE', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

src_L_DOMAIN_VALUE_DESCRIPTION_5ebbc1c7 = PythonOperator(
    task_id='Task_src_L_DOMAIN_VALUE_DESCRIPTION_5ebbc1c7',
    python_callable=JobExecutor().execute_task_query,
    op_args=["call DATA_OCEAN_LOG.ETL_METADATA.SP_WS_GP_UPDATE_src_L_DOMAIN_VALUE_DESCRIPTION_5ebbc1c7('{task_id}', 'DATA_OCEAN_LOG.WHERESCAPE_REPO', 'WS_repo_dp_old', '{create}', '{execution_user_name}', '{resource_group}');"],
    dag = dag,
    op_kwargs=sp_default_args
)

FINISH_JOB = DummyOperator(
    task_id='FINISH_JOB',
    trigger_rule='all_success'
)
src_L_CUSTOMER_MARKET_f837c325.set_upstream(INIT_JOB)
src_L_CUSTOMER_SEGMENT_44e44c5.set_upstream(INIT_JOB)
src_L_DELIVERY_DOCUMENT_DOCUMENT_ITEM_8a52ce05.set_upstream(INIT_JOB)
src_L_DELIVERY_DOCUMENT_HEADER_3eff372d.set_upstream(INIT_JOB)
src_L_DELIVERY_DOCUMENT_ITEM_DATA_4fb91ea5.set_upstream(INIT_JOB)
src_L_DISTRIBUTION_CHANNEL_DESCRIPTION_89b41aeb.set_upstream(INIT_JOB)
src_L_DOMAIN_VALUE_DESCRIPTION_5ebbc1c7.set_upstream(INIT_JOB)
src_L_MATERIAL_SALES_DATA_d52342a5.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_CHANGELOG_a1b38e67.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_ITEM_CHANGELOG_47908965.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_ITEM_PARTNER_9cd780a5.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_6e417ee5.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_763bc981.set_upstream(INIT_JOB)
src_L_SALES_ORGANIZATION_COMPANY_54fedee9.set_upstream(INIT_JOB)
src_L_SALES_ORGANIZATION_DESCRIPTION_f50913a7.set_upstream(INIT_JOB)
src_L_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_90815da5.set_upstream(INIT_JOB)
src_L_PURCHASING_DOCUMENT_HISTORY_470b3625.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_ccec2165.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_TYPE_DESCRIPTION_2_a8a5e138.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_HEADER_4_aae58fa.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b.set_upstream(INIT_JOB)
src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63.set_upstream([src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed])
src_L_SALES_DOCUMENT_DOCUMENT_ITEM_9_abf41f67.set_upstream([src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63])
src_L_SALES_DOCUMENT_HEADER_9dfada85.set_upstream([src_L_SALES_DOCUMENT_HEADER_4_aae58fa])
src_L_SALES_DOCUMENT_ITEM_DATA_4_deaf7320.set_upstream([src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b])
FINISH_JOB.set_upstream([src_L_CUSTOMER_MARKET_f837c325,	src_L_CUSTOMER_SEGMENT_44e44c5,	src_L_DELIVERY_DOCUMENT_DOCUMENT_ITEM_8a52ce05,	src_L_DELIVERY_DOCUMENT_HEADER_3eff372d,	src_L_DELIVERY_DOCUMENT_ITEM_DATA_4fb91ea5,	src_L_DISTRIBUTION_CHANNEL_DESCRIPTION_89b41aeb,	src_L_DOMAIN_VALUE_DESCRIPTION_5ebbc1c7,	src_L_MATERIAL_SALES_DATA_d52342a5,	src_L_SALES_DOCUMENT_CHANGELOG_a1b38e67,	src_L_SALES_DOCUMENT_ITEM_CHANGELOG_47908965,	src_L_SALES_DOCUMENT_ITEM_PARTNER_9cd780a5,	src_L_SALES_DOCUMENT_ITEM_SCHEDULE_LINE_6e417ee5,	src_L_SALES_DOCUMENT_REJECT_REASON_DESCRIPTION_763bc981,	src_L_SALES_ORGANIZATION_COMPANY_54fedee9,	src_L_SALES_ORGANIZATION_DESCRIPTION_f50913a7,	src_L_SALES_ORGANIZATION_DISTRIBUTION_CHANNEL_90815da5,	src_L_PURCHASING_DOCUMENT_HISTORY_470b3625,	src_L_SALES_DOCUMENT_ORDER_REASON_DESCRIPTION_ccec2165,	src_L_SALES_DOCUMENT_TYPE_DESCRIPTION_2_a8a5e138,	src_L_SALES_DOCUMENT_DOCUMENT_ITEM_434f1aed,	src_L_SALES_DOCUMENT_HEADER_4_aae58fa,	src_L_SALES_DOCUMENT_ITEM_DATA_27a8462b,	src_L_SALES_DOCUMENT_DOCUMENT_ITEM_5_abf41f63,	src_L_SALES_DOCUMENT_DOCUMENT_ITEM_9_abf41f67,	src_L_SALES_DOCUMENT_HEADER_9dfada85,	src_L_SALES_DOCUMENT_ITEM_DATA_4_deaf7320])
