from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
"""
this Airflow pipeline DAG inserts fact and dimension tables from S3 bucket, filling the our Amazon Redshift RDB, and finally running checks on the data for verification.
"""

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# populate the 'default_args' dictionary such that it will be passed on to airflow operators and fulfills provided guidelines: 
# ◆ The DAG does not have dependencies on past runs
# ◆ On failure, the task are retried 3 times
# ◆ Retries happen every 5 minutes
# ◆ Catchup is turned off
# ◆ Do not email on retry

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly' # or alternatively '0 * * * *'
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path_option='s3://udacity-dend/log_json_path.json',
    table='staging_events'   
)
        
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path_option='s3://udacity-dend/song_json_path.json',
    table='staging_songs'   
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    check_nulls=[] #pass column names in a list to check for null values
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Configure the task dependencies such that the graph looks like the following:
#
#                                                                      -> load_song_dimension_table -
#                -> stage_events_to_redshift                          /                              \
#               /                            \                       /-> load_user_dimension_table ---\
#start_operator                               -> load_songplays_table                                  -> run_quality_checks -> end_operator
#               \                            /                       \-> load_artist_dimension_table -/
#                -> stage_songs_to_redshift                           \                              /
#                                                                      -> load_time_dimension_table /

start_operator >> [stage_events_to_redshift, 
                   stage_songs_to_redshift] >> load_songplays_table >> [load_song_dimension_table, 
                                                                        load_user_dimension_table, 
                                                                        load_artist_dimension_table, 
                                                                        load_time_dimension_table] >> run_quality_checks >> end_operator

# albeit rather verbose configuration of DAG dependencies

# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift

# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table

# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table         

# load_user_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# run_quality_checks >> end_operator
