import lookups
import error_handler


def Execute_query(conn, query, data=None, return_result=False):
    try:
        with conn.cursor() as cur:
            if data:
                cur.execute(query, data)
            else:
                cur.execute(query)
            
            if return_result:
                result = cur.fetchall()
                return result
            else:
                conn.commit()
    except Exception as error:
        prefix = lookups.ErrorHandling.Excute_query_error.value
        suffix = str(error)
        error_handler.print_error(suffix,prefix)

def execute_sql_file(conn, file_path, return_result=False):
    with open(file_path, 'r') as file:
        sql_query = file.read()
        return Execute_query(conn, sql_query, return_result=return_result)