from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 check_stmts=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check_stmts  = check_stmts 
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        
        for stmt in self.check_stmts:
            
            #extracting table name
            table_name=stmt['query'].split(' ')[3]
            self.log.info(f"Data Quality checking for {table_name} table")
            
            #Get the number of records in the table
            result = int(redshift_hook.get_first(sql=stmt['query'])[0])
            
            print('Obtained result is '+str(result))
            print('Expected_Result is '+str(stmt['expected_result']))
            
            if int(result)>= int(stmt['expected_result']):     
                self.log.info(f"Data quality on table {table_name} check passed with {result} records")
            else:
                raise ValueError(f"Data quality check failed. {table_name} contained {result} rows")
            