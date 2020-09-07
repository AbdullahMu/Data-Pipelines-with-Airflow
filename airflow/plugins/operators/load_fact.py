from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # LoadFactOperator parameters:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql=sql

    def execute(self, context):
        # connect to Amazon Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Execute the insertion query on Redshift hook
        redshift.run(f"INSERT INTO {self.table} {self.sql}")
        self.log.info(f"LoadFactOperator implemented: Data inserted into {self.table} in Redshift ')
