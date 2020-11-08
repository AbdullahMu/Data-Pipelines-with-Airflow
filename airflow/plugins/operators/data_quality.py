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

        data_quality_check : list
            a list of dictionaries including the SQL query to be excuted for the check with key 'query' and its expected retured value with key 'expected_result' if other than 0
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_check=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # DataQualityOperator parameters:
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_check = data_quality_check

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

        # run futher checks if 'data_quality_check' is provided
        if self.data_quality_check:
            check_index = 0
            try:
                for check in self.data_quality_check:
                    check_query = check['query']
                    check_result = redshift_hook.get_records(check_query)
                    expected_result = check.get('expected_result', 0)  # get check['expected_result'] if exists, else assume 0
                    
                    if check_result = expected_result:
                        self.log.info(f"additional data quality ckeck {check_index} passed")
                    else:
                        self.log.info(f"additional data quality ckeck {check_index} failed, returned: {check_result}, expected: {expected_result}")
                    check_index += 1
                    
            except NameError:
                raise NameError("wrong data type for data qualtiy checks")

                
