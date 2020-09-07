from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    '''
        this operator loads dimension tables into our Amazon Redshift RDB

        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        table : str
            table name which data will be inserted to
            
        sql : str
            the sql query that will be used to insert the fact table
            
        truncate : bool
            flag indicats truncating inserted dimensions
    '''

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # LoadDimensionOperator parameters:
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate=truncate

    def execute(self, context):
        # connect to Amazon Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # format query for loading the dimension table
        if self.truncate:
            
            formatted_sql = f"TRUNCATE {self.table}; INSERT INTO {self.table} ({self.sql})"
            
        else:
            
            formatted_sql = f"INSERT INTO {self.table} ({self.sql})"
            
        # Execute the insertion query on Redshift hook
        redshift.run(formatted_sql)
        
        self.log.info(f'LoadDimensionOperator implemented: Dimension table loaded to {self.table}')
