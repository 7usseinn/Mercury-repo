import pandas as pd
import lookups
import error_handler
from database_handler import connect_to_db
import json
import requests

def return_data_as_dataframe(file_path, file_type, conn):
    try:

        if file_type == lookups.FileHandling.CSV.value:
           df = pd.read_csv(file_path)
        elif file_type == lookups.FileHandling.XLSX.value:
             df = pd.read_excel(file_path)
        elif file_type == lookups.FileHandling.JSON.value:
          with open(file_path, 'r') as json_file:
            df = json.load(json_file)
        elif file_type == lookups.FileHandling.API.value:        
            response = requests.get(file_path)
            if response.status_code == 200:
               df = response.json()
           
    except Exception as error:
        prefix = lookups.ErrorHandling.Data_handler_error.value
        suffix = str(error)
        error_handler.print_error(suffix,prefix)
        return None
    

def return_data_as_object(file_path, file_type, config_file):
    try:

        if file_type == lookups.FileHandling.CSV.value:
           data = pd.read_csv(file_path)
           return data.to_csv(index=False)
        elif file_type == lookups.FileHandling.XLSX.value:
             data = pd.read_excel(file_path)
             return data.to_csv(index=False)
        elif file_type == lookups.FileHandling.JSON.value:
          with open(file_path, 'r') as json_file:
             data = json.load(json_file)
          return json.dumps(data)
        elif file_type == lookups.FileHandling.API.value:        
            response = requests.get(file_path)
            if response.status_code == 200:
               data = response.json()
               return json.dumps(data)
           
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
