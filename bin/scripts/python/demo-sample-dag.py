from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# The main DAG file
# Author: Viswa Mohanty

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['viswa.mohanty@brillio.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'demo_sample',
    default_args=default_args,
    description='A simple demo DAG showing a typical Datawarehouse flow',
    schedule_interval=timedelta(days=1),
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
s1 = BashOperator(
    task_id='start_jobs',
    bash_command='echo "Starting the job"',
    dag=dag,
)

fw1 = BashOperator(
    task_id='Source_1_file_watcher',
    depends_on_past=False,
    bash_command='while [ ! -f /tmp/sleep.txt ]; do sleep 5; done',
    retries=3,
    dag=dag,
)

fw2 = BashOperator(
    task_id='Source_2_file_watcher',
    depends_on_past=False,
    bash_command='while [ ! -f /tmp/sleep.txt ]; do sleep 5; done',
    retries=3,
    dag=dag,
)

fw3 = BashOperator(
    task_id='Source_3_file_watcher',
    depends_on_past=False,
    bash_command='while [ ! -f /tmp/sleep.txt ]; do sleep 5; done',
    retries=3,
    dag=dag,
)

quality1 = BashOperator(
    task_id='quality_checks_on_source_1_file',
    depends_on_past=False,
    bash_command='echo "Put command for quality checks for source 1 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

quality2 = BashOperator(
    task_id='quality_checks_on_source_2_file',
    depends_on_past=False,
    bash_command='echo "Put command for quality checks for source 2 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

quality3 = BashOperator(
    task_id='quality_checks_on_source_3_file',
    depends_on_past=False,
    bash_command='echo "Put command for quality checks for source 3 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

stage1 = BashOperator(
    task_id='staging_load_for_source_1',
    depends_on_past=False,
    bash_command='echo "Put command for staging load for source 1"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

stage2 = BashOperator(
    task_id='staging_load_for_source_2',
    depends_on_past=False,
    bash_command='echo "Put command for staging load for source 2"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

stage3 = BashOperator(
    task_id='staging_load_for_source_3',
    depends_on_past=False,
    bash_command='echo "Put command for staging load for source 3"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

transform1 = BashOperator(
    task_id='transformations_and_load_for_source_1',
    depends_on_past=False,
    bash_command='echo "Put command for transformations and final load for source 1"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

transform2 = BashOperator(
    task_id='transformations_and_load_for_source_2',
    depends_on_past=False,
    bash_command='echo "Put command for transformations and final load for source 2"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

transform3 = BashOperator(
    task_id='transformations_and_load_for_source_3',
    depends_on_past=False,
    bash_command='echo "Put command for transformations and final load for source 3"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

Agg1 = BashOperator(
    task_id='Aggregation_for_sources_2_and_3',
    depends_on_past=False,
    bash_command='echo "Put command for aggregation for source 2 & 3"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

prep_archve1 = BashOperator(
    task_id='prepare_for_archive_source_1_file',
    depends_on_past=False,
    bash_command='echo "Put command for readiness to archive source 1 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

prep_archve2 = BashOperator(
    task_id='prepare_for_archive_source_2_file',
    depends_on_past=False,
    bash_command='echo "Put command for readiness to archive source 2 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

prep_archve3 = BashOperator(
    task_id='prepare_for_archive_source_3_file',
    depends_on_past=False,
    bash_command='echo "Put command for readiness to archive source 3 file"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

archive_all = BashOperator(
    task_id='archive_all_files',
    depends_on_past=False,
    bash_command='echo "Put command for archive all files"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

mail_load1 = BashOperator(
    task_id='mail_load_completion_for_source_1',
    depends_on_past=False,
    bash_command='echo "Put command for mailing batch completion for source 1"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

mail_load2 = BashOperator(
    task_id='mail_load_completion_for_source_2',
    depends_on_past=False,
    bash_command='echo "Put command for mailing batch completion for source 2"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

mail_load3 = BashOperator(
    task_id='mail_load_completion_for_source_3',
    depends_on_past=False,
    bash_command='echo "Put command for mailing batch completion for source 3"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

mail_agg = BashOperator(
    task_id='mail_load_completion_for_agg',
    depends_on_past=False,
    bash_command='echo "Put command for mailing batch completion for aggregation"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)

mail_all_archive = BashOperator(
    task_id='mail_archive_completion',
    depends_on_past=False,
    bash_command='echo "Put command for mailing archive completion"',
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)


s1 >> [fw1, fw2, fw3]
quality1 >> stage1 >> transform1 >> mail_load1
quality2 >> stage2 >> transform2 >> mail_load2
quality3 >> stage3 >> transform3 >> mail_load3
fw1 >> [quality1, prep_archve1]
fw2 >> [quality2, prep_archve2]
fw3 >> [quality3, prep_archve3]
Agg1 << [transform2, transform3]
Agg1 >> mail_agg
archive_all << [prep_archve1, prep_archve2, prep_archve3]
archive_all >> mail_all_archive
