import data_handler
import database_handler


def execute_posthook():
    db_session = database_handler.connect_to_db()
    try:
        db_session = database_handler.connect_to_db()
        table_name = 'stg_kaggle_fraud'
        schema_name = 'bank_schema'
        truncate_query = data_handler.return_truncate_statement(schema_name,table_name)
        database_handler.execute_query(db_session, truncate_query)
    except Exception as e:
        print(str(e))
    finally:
        if db_session:
            database_handler.close_connection(db_session)
