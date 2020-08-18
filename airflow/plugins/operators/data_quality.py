from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="", 
                 sql_query="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query # e.g. SELECT COUNT(*) FROM {self.table_name}
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info("Retrieving Redshift Credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connected to Redshift")
        
        self.log.info("Data quality check...")
        records = redshift_hook.get_records(self.sql_query)
        
        if records [0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality check has failed. \
                {results[0][0]} does not equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality check has passed")
        
        
