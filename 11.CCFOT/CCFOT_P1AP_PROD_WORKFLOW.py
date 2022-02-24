# Created : 02/11/2022 16:12:38
# Start job : 09/25/2021 00:01:00

from operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Variable
from airflow.utils.dates import days_ago

CRON = '5 0 * * *'

dag_default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'max_active_runs': 1,
    'retries': 0
}

sp_default_args = {
    'create': 'true',
    'execution_user_name': 'AIRFLOW',
    'resource_group': 'ETL_XS',
    'restarted': 'false',
    'workflow_name': 'CCFOT_P1AP_PROD_WORKFLOW'
}

dag = DAG(
    'CCFOT_P1AP_PROD_WORKFLOW',
    description='CCFOT_P1AP_PROD_WORKFLOW',
    start_date=days_ago(1),
    default_args=dag_default_args,
    schedule_interval=CRON,
    catchup=False,
    tags=['CCFOT']
)

INIT_WORKFLOW = DummyOperator(
    task_id='INIT_WORKFLOW'
)

trigger_CCFOT_P1AP_PROD_0_START = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_0_START",
    trigger_dag_id='CCFOT_P1AP_PROD_0_START',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_1_LOAD = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_1_LOAD",
    trigger_dag_id='CCFOT_P1AP_PROD_1_LOAD',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_2_STAGE = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_2_STAGE",
    trigger_dag_id='CCFOT_P1AP_PROD_2_STAGE',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_3_HUB_MULTI = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_3_HUB_MULTI",
    trigger_dag_id='CCFOT_P1AP_PROD_3_HUB_MULTI',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_3_LINK_MULTI = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_3_LINK_MULTI",
    trigger_dag_id='CCFOT_P1AP_PROD_3_LINK_MULTI',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_3_SAT_SINGLE = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_3_SAT_SINGLE",
    trigger_dag_id='CCFOT_P1AP_PROD_3_SAT_SINGLE',
    parent_dag_id=dag.dag_id,
    dag=dag
)

trigger_CCFOT_P1AP_PROD_4_PRES = TriggerDagRunOperator(
    task_id="trigger_CCFOT_P1AP_PROD_4_PRES",
    trigger_dag_id='CCFOT_P1AP_PROD_4_PRES',
    parent_dag_id=dag.dag_id,
    dag=dag
)

FINISH_WORKFLOW = DummyOperator(
    task_id='FINISH_WORKFLOW',
    trigger_rule='all_success'
)

trigger_CCFOT_P1AP_PROD_0_START.set_upstream(INIT_WORKFLOW)
trigger_CCFOT_P1AP_PROD_1_LOAD.set_upstream(trigger_CCFOT_P1AP_PROD_0_START)
trigger_CCFOT_P1AP_PROD_2_STAGE.set_upstream(trigger_CCFOT_P1AP_PROD_1_LOAD)
trigger_CCFOT_P1AP_PROD_3_HUB_MULTI.set_upstream(trigger_CCFOT_P1AP_PROD_2_STAGE)
trigger_CCFOT_P1AP_PROD_3_LINK_MULTI.set_upstream(trigger_CCFOT_P1AP_PROD_2_STAGE)
trigger_CCFOT_P1AP_PROD_3_SAT_SINGLE.set_upstream(trigger_CCFOT_P1AP_PROD_2_STAGE)
trigger_CCFOT_P1AP_PROD_4_PRES.set_upstream(
    [trigger_CCFOT_P1AP_PROD_3_HUB_MULTI, trigger_CCFOT_P1AP_PROD_3_LINK_MULTI, trigger_CCFOT_P1AP_PROD_3_SAT_SINGLE])
FINISH_WORKFLOW.set_upstream(trigger_CCFOT_P1AP_PROD_4_PRES)
