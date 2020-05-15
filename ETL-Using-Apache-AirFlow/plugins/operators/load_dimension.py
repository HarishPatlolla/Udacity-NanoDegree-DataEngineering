from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
        COMMIT;
    """

    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 mode = 'append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading dimension table {self.table} in Redshift")
        
        #If mode='truncate'; truncate first
        if self.mode == 'truncate':
            self.log.info("Truncating the data from {} dimension table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))   
            self.log.info("Deletion complete")
         
        #Insert 
        self.log.info("Insert data from fact table into {} dimension table".format(self.table))
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.load_sql_stmt)
        redshift.run(formatted_sql)