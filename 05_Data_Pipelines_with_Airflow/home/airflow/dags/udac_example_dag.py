from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers.sql_queries import Queries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 9, 11),
    'end_date': datetime(2020, 9, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udacity-airflow-project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
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
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=Queries.songplay_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=Queries.song_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=Queries.user_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=Queries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    sql_query=Queries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
	redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    tables=[
        "songplays",
        "users",
        "songs",
        "artists",
        "time"
    ]
  
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

		
# First Step
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

# Second Step
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

# 3rd Step
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# 4th Step
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Final Step
run_quality_checks >> end_operator