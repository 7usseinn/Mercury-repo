import database_handler
import data_handler
import pandas as pd
import glob

def execute(db_session):
   db_session = database_handler.connect_to_db()
   execute_prehook_sql(db_session)
   create_stg_table(db_session)
   database_handler.close_connection(db_session)    

def execute_prehook_sql(db_session):
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
    fraud_dataframe['trans_date_trans_time'] = pd.to_datetime(fraud_dataframe['trans_date_trans_time'])
    last_execution_date = data_handler.get_last_execution_time(db_session)
    last_execution_date = pd.to_datetime(last_execution_date)
    filtered_fraud_dataframe = fraud_dataframe[fraud_dataframe['trans_date_trans_time'] > last_execution_date]
    insert_statements = data_handler.return_insert_statement(filtered_fraud_dataframe,'stg_kaggle_fraud','bank_schema')

    for insert_statement in insert_statements:
     database_handler.execute_query(db_session, insert_statement)

    if not filtered_fraud_dataframe.empty:
     last_date = filtered_fraud_dataframe['trans_date_trans_time'].iloc[-1]
     insert_into_watermark = data_handler.return_insert_statement_for_watermark(last_date,'etl_watermark','bank_schema')
     database_handler.execute_query(db_session,insert_into_watermark)
    else:
     print("The filtered_fraud_dataframe is empty. Check your filtering conditions.")
    
