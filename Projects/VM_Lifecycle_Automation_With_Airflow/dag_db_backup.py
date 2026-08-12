import airflow 
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'db_backup',
    default_args=default_args,
    description='Database Backup',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=10),
)

stopvm = BashOperator(
    task_id='stopvm',
    bash_command='gcloud compute instances stop database --zone=us-central1-a',
    dag=dag,
    depends_on_past=False,
    do_xcom_push=False)

create_image = BashOperator(
    task_id='create_image',
    bash_command='gcloud compute machine-images create database-image --source-instance=database --source-instance-zone=us-central1-a',
    dag=dag,
    depends_on_past=False,
    do_xcom_push=False)

startvm = BashOperator(
    task_id='startvm',
    bash_command='gcloud compute instances start database --zone=us-central1-a',
    dag=dag,
    trigger_rule='one_success',
    depends_on_past=False,
    do_xcom_push=False)

create_snapshot = BashOperator(
    task_id='create_snapshot',
    bash_command='gcloud compute snapshots create databasesnapshot --source-disk=database --source-disk-zone=us-central1-a',
    dag=dag,
    trigger_rule='one_failed',
    depends_on_past=False,
    do_xcom_push=False)

stopvm >> create_image >> startvm
create_image >> create_snapshot 
create_snapshot >> startvm
