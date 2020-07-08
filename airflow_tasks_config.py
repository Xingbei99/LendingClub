import base64
import json
import os
from datetime import datetime, timedelta
from time import time
from airflow import DAG
from airflow.utils import trigger_rule
from airflow.operators import PythonOperator
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataProcSparkOperator, DataprocClusterDeleteOperator

dag_name = 'lendingclubdag'.strip()

def push_cluster_name(**kwargs):
    ti = kwargs['ti']
    cluster_name = dag_name[:27] + '-' + str(int(round(time() * 100)))
    ti.xcom_push(key='cluster_name', value=cluster_name)

# DAG-level settings.
with DAG(dag_id=dag_name,
         schedule_interval='@daily',
         start_date=datetime.strptime('2020-04-07 00:00:00', "%Y-%m-%d %H:%M:%S"),
         
         max_active_runs=1,
         concurrency=1,
         default_args={
            'project_id': 'silicon-parity-282607',
            'email': 'test@gmail.com',
            'email_on_failure': True,
            'email_on_retry': False
         }) as dag:

    push_cluster_name = PythonOperator(dag=dag, task_id="push-cluster-name", provide_context=True, python_callable=push_cluster_name)

    # The task of creating a cluster.
    dataproc_create_cluster =  DataprocClusterCreateOperator(
        task_id='dataproc-create-cluster',
        project_id='silicon-parity-282607',
        region='us-central1',
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2',
        cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
        num_workers=2)

    # The task of running the Spark job.
    dataproc_spark_process = DataProcSparkOperator(
        task_id='dataproc-test',
        dataproc_spark_jars=['gs://lendingclub12/LendingClub-assembly-0.1.jar'],
        main_class='p2p_data_analysis.spark.LoanDataAnalyzer',
        job_name='loan',
        region='us-central1',
        cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
        arguments=["gs://lendingclub12/LoanStats_2019Q1.csv",
 "gs://lendingclub12/RejectStats_2019Q1.csv",
 "gs://lendingclub12/output"]
)

    # The task of deleting the cluster.
    dataproc_destroy_cluster = DataprocClusterDeleteOperator(
        task_id='dataproc-destroy-cluster',
        project_id='silicon-parity-282607',
        region='us-central1',
        cluster_name='{{ ti.xcom_pull(key="cluster_name", task_ids="push-cluster-name") }}',
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    # Set the dependency chain.
    push_cluster_name >> dataproc_create_cluster >> dataproc_spark_process >> dataproc_destroy_cluster