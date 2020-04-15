from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Optional

from helpers.sql_queries import SqlQueries


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 table: str,
                 insert_mode: Optional[str] = "delete-insert",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_mode = insert_mode
        create_table_queries = {
            "artists": SqlQueries.artist_table_create,
            "songs": SqlQueries.song_table_create,
            "time": SqlQueries.time_table_create,
            "users": SqlQueries.user_table_create
        }
        insert_queries = {
            "artists": SqlQueries.artist_table_insert,
            "songs": SqlQueries.song_table_insert,
            "time": SqlQueries.time_table_insert,
            "users": SqlQueries.user_table_insert
        }
        self.create_table_query = create_table_queries[table]
        self.insert_query = insert_queries[table]

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Creating the {} dimension table if not exists.".format(self.table))
        redshift.run(self.create_table_query)
        if self.insert_mode == 'delete-insert':
            self._delete_conflicting_rows(redshift)
        self.log.info("Filling the table.")
        redshift.run(self.insert_query)
        self.log.info("Done with {}.".format(self.table))
    
    def _delete_conflicting_rows(self, redshift: PostgresHook):
        primary_key_table = {
            'artists': 'artist_id',
            'songs': 'song_id',
            'time': 'start_time',
            'users': 'user_id',
        }
        staging_primary_key_mirrors = {
            'artists': 'artist_id',
            'songs': 'song_id',
            'time': 'start_time',
            'users': 'userId'
        }
        if self.table == 'artists' or self.table == 'songs':
            staging_table = 'staging_songs'
        elif self.table == 'users':
            staging_table = 'staging_logs'
        elif self.table == 'time': 
            staging_table = 'songplays'
        else: 
            raise ValueError(f"Table {self.table} does not exist!")
        table_pk = primary_key_table.get(self.table)
        staging_pk = staging_primary_key_mirrors.get(self.table)
        self.log.info("Deleting records where {}.{} = {}.{}.".format(self.table, table_pk, staging_table, staging_pk))
        format_args = [self.table, staging_table, self.table, table_pk, staging_table, staging_pk]
        redshift.run(SqlQueries.delete_conflicting_rows.format(*format_args))
