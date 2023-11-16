import database_handler
import data_handler
import glob

def execute(db_session):
    sql_files = glob.glob("**/*.sql")

    for sql_file in sql_files:
         
        file_name = sql_file.split("\\")[-1]
         
        if "_prehook" in file_name:
            query = None
            print(file_name)   
            
            with open(sql_file, "r") as f:
                query = f.read()
            database_handler.execute_query(db_session, query)
            db_session.commit()



def create_stg_table(db_session):
    fraud_dataframe = data_handler.return_data_as_dataframe(r'C:\Users\user\Desktop\jem3a\Projet_Info_Data\fraud.csv','csv', db_session)
    stg_table_query = data_handler.create_staging_table(fraud_dataframe,'bank_schema','stg_kaggle_fraud')
    database_handler.execute_query(db_session,stg_table_query)
    insert_statements = data_handler.return_insert_statement(fraud_dataframe,'stg_kaggle_fraud','bank_schema')

    for insert_statement in insert_statements:
     database_handler.execute_query(db_session, insert_statement)

    last_date = fraud_dataframe['trans_date_trans_time'].iloc[-1]
    insert_into_watermark = data_handler.return_insert_statement_for_watermark(last_date,'etl_watermark','bank_schema')
    database_handler.execute_query(db_session,insert_into_watermark)
    
