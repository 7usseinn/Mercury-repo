import psycopg2
import lookups
import error_handler

def connect_to_db(config_file):
    db_session = None
    try:

        db_host = 'localhost'
        db_name = 'Bank-data'
        db_user = 'postgres'
        db_pass = 'hajhaSSan1'

        db_session = psycopg2.connect(
            host = db_host,
            database = db_name,
            user = db_user,
            password = db_pass,
            port = 5432
        )
    except Exception as error:
        prefix = lookups.ErrorHandling.DB_CONNECTION_ERROR.value
        suffix = str(error)
        error_handler.print_error(suffix, prefix)
    finally:
        return db_session