import database_handler
import glob

def execute(db_session):
    db_session = database_handler.connect_to_db()
    try:
        sql_files = glob.glob("**/*.sql")

        for sql_file in sql_files:
            file_name = sql_file.split("\\")[-1]

            if "_hook" in file_name:
                query = None
                print(file_name)

                with open(sql_file, "r") as f:
                    query = f.read()
                database_handler.execute_query(db_session, query)
                db_session.commit()

    except Exception as e:
        print(str(e))
    finally:
        if db_session:
         database_handler.close_connection(db_session)
