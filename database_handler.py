import psycopg2
import lookups
import error_handler
import json

def connect_to_db():
    db_session = None
    config_file = 'config.json'
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
        level = lookups.ErrorLevel.ERROR.value
        message = 'Error happenend'
        error_handler.print_error(suffix, prefix,level,message)
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
        level = lookups.ErrorLevel.ERROR.value
        message = 'Error happenend'
        error_handler.print_error(suffix, prefix,level,message)



def close_connection(db_session):
    try:
        if db_session is not None:
            db_session.close()
            print("Database connection closed successfully")
    except Exception as error:
        print(str(error))