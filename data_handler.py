import pandas as pd
import lookups
import error_handler

def read_file(file_path, file_type, conn):
    try:

        if file_type == "csv":
           return read_file(file_path)
        elif file_type == "xlsx":
           return read_file(file_path)
        elif file_type == "json":
            return read_file(file_path)
           
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
        return None


    
def return_sql_file(conn, file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return return_query(conn, sql_query)