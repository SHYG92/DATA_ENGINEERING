from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'retries': 1,
    'retry_delay': 400,
    'catchup': True
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Data Pipeline with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    file_type="json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A",
    json_path="auto",
    file_type="json"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = 'redshift',
    sql_query = SqlQueries.songplay_table_insert, 
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    append=True,
    sql_stmt=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    append=True,
    sql_stmt=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    append=True,
    sql_stmt=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dimension_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    append=True,
    sql_stmt=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time']
    data_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
                 {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
                ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


## Tasks Dependincies
## 01
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

## 02
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

## 03
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

## 04
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

## 05
run_quality_checks >> end_operator
