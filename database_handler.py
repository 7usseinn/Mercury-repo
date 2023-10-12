import psycopg2
import lookups
import error_handler
import json

def connect_to_db(config_file):
    db_session = None
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)

        db_session = psycopg2.connect(
            host=config["db_host"],
            database=config["db_name"],
            user=config["db_user"],
            password=config["db_pass"],
            port=config["db_port"]
        )
    except Exception as error:
        prefix = lookups.ErrorHandling.DB_CONNECTION_ERROR.value
        suffix = str(error)
        error_handler.print_error(suffix, prefix)
    finally:
        return db_session
    

def execute_query(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
    except Exception as error:
        prefix = lookups.ErrorHandling.Excute_query_error.value
        suffix = str(error)
        error_handler.print_error(suffix,prefix)

def execute_sql_file(conn, file_path):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return execute_query(conn, sql_query)