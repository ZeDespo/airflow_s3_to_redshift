from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta

from operators.data_quality import DataQualityOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime.now(),
    'sla': timedelta(minutes=30),
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data from S3 to Redshift with Airflow',
    schedule_interval='@hourly',
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    iam_role="Redshift_Read_S3",
    redshift_conn_id="redshift",
    s3_json_structure_path="s3://udacity-redshift/log_paths.json",
    s3_data_path="s3://udacity-dend/log_data",
    table='staging_logs'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id="aws_credentials",
    iam_role="Redshift_Read_S3",
    redshift_conn_id="redshift",
    s3_json_structure_path="s3://udacity-redshift/song_paths.json",
    s3_data_path="s3://udacity-dend/song_data",
    table='staging_songs'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['users', 'songs', 'artists', 'time', 'songplays']
)

end_operator = PostgresOperator(
    task_id='End_execution',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
        BEGIN;
        DROP TABLE staging_songs;
        DROP TABLE staging_logs
        COMMIT;
    """
)  # For demonstration purposes.

# Ordering task steps
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator