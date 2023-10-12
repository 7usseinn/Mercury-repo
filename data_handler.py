import pandas as pd
import lookups
import error_handler

def read_file(file_path):
    try:

        if file_path.endswith(lookups.FileHandling.CSV.value):
           return pd.read_csv(file_path)
        if file_path.endswith(lookups.FileHandling.XLSX.value):
            return pd.read_excel(file_path)
    
        
    except Exception as error:
        prefix = lookups.ErrorHandling.Data_handler_error.value
        suffix = str(error)
        error_handler.print_error(suffix,prefix)
        return None
    

def return_query(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()
            return result
    except Exception as error:
        prefix = lookups.ErrorHandling.Excute_query_error.value
        suffix = str(error)
        error_handler.print_error(suffix,prefix)


    
def return_sql_file(conn, file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return return_query(conn, sql_query)