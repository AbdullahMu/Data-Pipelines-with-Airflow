from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")
        self.log.info("Getting Redshift Credentials")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Created Redshift connection")
                 
        self.log.info(f'Loading data into {self.table} fact table in Redshift...')  
        formatted_sql_query = f'''
            INSERT INTO {self.table}
            {self.sql_query}
        '''
        redshift_hook.run(formatted_sql_query)        
        self.log.info("Data loaded into {table} successfully".format(table=self.table))
        
