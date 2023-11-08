import pandas as pd
import lookups
import error_handler
from database_handler import connect_to_db
import json
import requests
import database_handler

def return_data_as_dataframe(file_path, file_type, conn=None):
    df = None
    try:

        if file_type == lookups.FileHandling.CSV.value:
           df = pd.read_csv(file_path)
        elif file_type == lookups.FileHandling.XLSX:
             df = pd.read_excel(file_path)
        elif file_type == lookups.FileHandling.JSON:
          with open(file_path, 'r') as json_file:
            df = json.load(json_file)
        elif file_type == lookups.FileHandling.API:        
            response = requests.get(file_path)
            if response.status_code == 200:
               df = response.json()
        elif file_type == lookups.FileHandling.SQL:
             query = return_sql_file(file_path)
             df = pd.read_sql(query,conn)
    except Exception as error:
        prefix = lookups.ErrorHandling.Data_handler_error.value
        suffix = str(error)
        level = lookups.ErrorLevel.ERROR.value
        message = 'Error happenend'
        error_handler.print_error(suffix,prefix,level,message)
        return None
    finally:
        return df

    

def return_sql_file(file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return sql_query 
    

def return_create_statement_from_df(dataframe, schema_name, table_name):
    type_mapping = {
        'int64':'INT',
        'float64':'FLOAT',
        'object':'TEXT',
        'datetime64[ns]':'TIMESTAMP'
    }
    fields = []
    for column, dtype in dataframe.dtypes.items():
        sql_type = type_mapping.get(str(dtype), 'TEXT')
        fields.append(f"{column} {sql_type}")
   
    create_table_statement = f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ( \n"
    create_table_statement += "ID SERIAL PRIMARY KEY,\n"
    create_table_statement += ',\n'.join(fields)
    create_table_statement += ");"
    return create_table_statement


def return_insert_statement(dataframe, table_name, schema):
    columns = ','.join(dataframe.columns)
    insert_statements = []

    for index, row in dataframe.iterrows():
        values_list = []
        for val in row.values:
            val_type = str(type(val))
            if val_type == lookups.HandledType.TIMESTAMP.value:
                values_list.append(str(val))
            elif val_type == lookups.HandledType.STRING.value:
                values_list.append(f"'{val}'")
            elif val_type == lookups.HandledType.LIST.value:
                val_item = ';'.join(val)
                values_list.append(f"'{val_item}'")
            else:
                values_list.append(str(val))

        values = ', '.join(values_list)
        insert_statement = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({values});"
        insert_statements.append(insert_statement)

    return insert_statements

def create_staging_table(df, schema_name, table_name):
    
 data_type = {
    'trans_date_trans_time': [''],
    'cc_num':[0.0],
    'merchant': [''],
    'category': [''],
    'amt': [0.0],
    'first': [''],
    'last': [''],
    'gender': [''],
    'street': [''],
    'city': [''],
    'state': [''],
    'zip': [0],
    'lat':[0.0],
    'long':[0.0],
    'city_pop':[0],
    'job':[''],
    'dob':[''],
    'trans_num':[''],
    'unix_time':[0],
    'merch_lat':[0.0],
    'merch_long':[0.0],
    'is_fraud':[0]
 }

 df = pd.DataFrame(data_type)
 create_statement = return_create_statement_from_df(df, schema_name, table_name)
 return create_statement