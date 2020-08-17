from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, 
                               LoadFactOperator, 
                               LoadDimensionOperator, 
                               DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

"""
this Airflow pipeline runs the ETL tasks: staging the data, filling the data warehouse, and running checks on the data as the final step.

"""

# These args will get passed on to each operator
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 9),
    'email': ['aalghamdi@me.com'],
    'email_on_failure' : True,
    'email_on_retry':False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past' : False
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
          description='Extract, Load, and Transform data from S3 to Redshift with Airflow',
          schedule_interval='@hourly' # or alternatively '0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,    
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials', 
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path_option='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,    
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path_option='auto'    
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift', 
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift', 
    table='users',
    sql_query=SqlQueries.user_table_insert,
    insert_mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift', 
    table='songs',
    sql_query=SqlQueries.song_table_insert,
    insert_mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift', 
    table='artists',
    sql_query=SqlQueries.artist_table_insert,
    insert_mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift', 
    table='time',
    sql_query=SqlQueries.time_table_insert,
    insert_mode='truncate'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=[
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'expected_result': 0 }, 
        { 'check_sql': 'SELECT COUNT(DISTINCT "level") FROM public.songplays', 'expected_result': 2 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.artists WHERE name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songs WHERE title IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL', 'expected_result': 0 },
        { 'check_sql': 'SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users u ON u.userid = sp.userid  WHERE u.userid IS NULL', \
         'expected_result': 0 }
    ],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting up DAG dependencies

start_operator >> [stage_events_to_redshift,
                   stage_songs_to_redshift] >> load_songplays_table >> [load_user_dimension_table,
                                                                       load_song_dimension_table,
                                                                       load_artist_dimension_table,
                                                                       load_time_dimension_table] >> run_quality_checks >> end_operator 
# alternative1

# start_operator.set_downstream([stage_events_to_redshift,
#                    stage_songs_to_redshift]).set_downstream(
#     load_songplays_table).set_downstream(
#     [load_user_dimension_table, load_song_dimension_table,load_artist_dimension_table, load_time_dimension_table].set_downstream(run_quality_checks).set_downstream(end_operator)
    
# alternative2: albeit rather verbose configuration of DAG dependencies

# start_operator >> stage_events_to_redshift
# start_operator >> stage_songs_to_redshift

# comment: collated above in the list [stage_events_to_redshift, stage_songs_to_redshift]

# stage_events_to_redshift >> load_songplays_table
# stage_songs_to_redshift >> load_songplays_table

# load_songplays_table >> load_user_dimension_table
# load_songplays_table >> load_song_dimension_table
# load_songplays_table >> load_artist_dimension_table
# load_songplays_table >> load_time_dimension_table         

# comment: collated above in the list [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]

# load_user_dimension_table >> run_quality_checks
# load_song_dimension_table >> run_quality_checks
# load_artist_dimension_table >> run_quality_checks
# load_time_dimension_table >> run_quality_checks

# run_quality_checks >> end_operator