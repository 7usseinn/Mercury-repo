import pandas as pd
import lookups
import error_handler
from database_handler import connect_to_db
import json
import requests

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
            result = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
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



def return_data_as_object(file_path, file_type, config_file):
    db_session = connect_to_db(config_file)

    supported_file_types = ['csv', 'excel', 'json', 'api']

    if file_type not in supported_file_types:
         print("Unsupported type!")
         return -1 

    if file_type == 'csv':
        data = pd.read_csv(file_path)
    elif file_type == 'excel':
        data = pd.read_excel(file_path)
    elif file_type == 'json':
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
    elif file_type == 'api':
        
        response = requests.get(file_path)
        if response.status_code == 200:
            data = response.json()
        else:
            print("failed to retrieve data from API")
            return -2

    return data