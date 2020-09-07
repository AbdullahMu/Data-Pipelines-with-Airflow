from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    
    '''
        this operator performs quality checks on data in our Amazon Redshift RDB after ETL
        the operator is heavily influenced by the HasRowsOperator used in lesson excercises

        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        column_null_check : list
            columns names which will go through futher checks, counting the numbers of nulls in them
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="",
                 column_null_check=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # DataQualityOperator parameters:
        self.redshift_conn_id = redshift_conn_id
        self.column_null_check = column_null_check

    def execute(self, context):
        # connect to Amazon Redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # run SELECT COUNT(*) query to retrieve the number of rows in public.songplays table
        records = redshift_hook.get_records("SELECT COUNT(*) FROM public.songplays ")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError("Data quality check failed. songplays table returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError("Data quality check failed. songplays contained 0 rows")
        self.log.info(f"DataQualityOperator implemented: Data quality on table songplays check passed with {records[0][0]} records")
        
        # run futher checks on the list of columns provided
        if column_null_check:
            
            null_records = 0
            
            for column in column_null_check:
                null_query = f'''
                                 SELECT COUNT(*) FROM public.songplays WHERE {column} IS NULL
                             '''
                null_records += redshift_hook.get_records(null_query)
                
        self.log.info(f"table songplays has {null_records[0][0]} null records")
