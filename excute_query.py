import lookups
import error_handler


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