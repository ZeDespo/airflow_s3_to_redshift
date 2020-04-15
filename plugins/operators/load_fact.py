from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
import time

from helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating the songplay fact table if not exists.")
        redshift.run(SqlQueries.songplays_table_create)
        self.log.info("Filling the fact table.")
        redshift.run(SqlQueries.songplays_table_insert) 
        self.log.info("Done with songplays")
