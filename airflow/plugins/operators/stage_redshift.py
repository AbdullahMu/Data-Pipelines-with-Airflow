from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
     '''
        this operator loads data for S3 bucket in thier JSON and transform them into columnar form and load it to our Amazon Redshift RDB

        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        aws_credentials_id : str
            IAM role credentials
            
        table : str
            table name which data will be inserted to
            
        sql : str
            the sql query that will be used to insert the fact table
            
        s3_bucket : str
            name of the S3 bucket
            
        s3_key : str
            name of the subdirectory S3 bucket where is the target data
            
        json_path_option : str
            path where the JSON file form S3 is loaded 
            
        ignore_headers : int 
            number of rows ingnored as headers in the copy sql query
    '''
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        FORMAT as json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 json_path_option="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # StageToRedshiftOperator parameters:
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.json_path_option = json_path_option

    def execute(self, context):
        # retrieve AWS and Redshift credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # connect to Amazon Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # clear data from destination Redshift table
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # copy data from S3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        # format the copying query using provided parameters
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
            json_path_option = self.json_path_option
        )
        # Execute the formatted copying query on Redshift hook
        redshift.run(formatted_sql)
        
        self.log.info(f'StageToRedshiftOperator implemented: data in S3 {self.table} copied into Amazon Redshift')
