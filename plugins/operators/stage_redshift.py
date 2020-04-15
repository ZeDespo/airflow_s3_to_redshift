from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers.sql_queries import SqlQueries
# from plugins.sql_queries import create_staging_log_table, create_staging_song_table, copy_from_s3_to_staging


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id: str,
                 iam_role: str,
                 s3_data_path: str,
                 s3_json_structure_path: str,
                 redshift_conn_id: str,
                 table: str,
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws, self.redshift = aws_credentials_id, redshift_conn_id
        self.s3_path, self.table = s3_data_path, table
        self.json_structure = s3_json_structure_path
        self.iam_role = iam_role
        if self.table == "staging_logs": 
            self.create_temp_table = SqlQueries.create_staging_log_table 
        elif self.table == "staging_songs":
            self.create_temp_table = SqlQueries.create_staging_song_table
        else:
            error = "The staging table name {} is not valid. Expected staging_logs or staging_songs".format(table)
            self.log.critical(error)
            raise ValueError(error)
        
    def execute(self, context):
        aws_hook = AwsHook(self.aws)
        aws_iam_arn = aws_hook.expand_role(self.iam_role)
        redshift = PostgresHook(postgres_conn_id=self.redshift)
        self.log.info("Creating table {} if not exists".format(self.table))
        redshift.run(self.create_temp_table)
        self.log.info("Clearing all rows from {}".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Copying data from {} to {}.".format(self.s3_path, self.table))
        sql = SqlQueries.copy_from_s3_to_staging.format(self.table, self.s3_path, aws_iam_arn, self.json_structure)
        redshift.run(sql)
        self.log.info("{} is now populated.".format(self.table))
