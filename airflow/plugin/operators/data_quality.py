from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks="",
                 tables_names=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_names = tables_names
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        failing_tests = []
        
        for check in self.dq_checks:
            
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            
            records_query = redshift_hook.get_records(sql)[0]
            
            if expected_result != records_query[0]:
                failing_tests.append(sql)
                self.log.info('Data Quality validation status: FAILED')
                self.log.info(failing_tests)
                raise ValueError('Data Quality validation status: FAILED')
            
        for table_name in self.tables_names:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data Quality validation status: FAILED for table : {table_name}. No results were returned!")
                raise ValueError(f"Data Quality validation status: FAILED for table : {table_name}. No results were returned!")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data Quality validation status: FAILED for table : {table_name}.The table contained 0 rows")
            self.log.info(f"Data Quality validation status: SUCCESSFUL  for table : {table_name}. {records[0][0]} records were returned!")  
       
        
     
