from enum import Enum

class ErrorHandling(Enum):
    DB_connection_error = "Failed to connect to database (database_handler)"
    Excute_query_error = "Error executing query (excute_query)"
    Data_handler_error = "Unsupported file format (data_handler)"


class FileHandling(Enum):
    CSV = 'csv'
    XLSX = 'xlsx'
    JSON = 'json'
    API = 'api'


class HandledType(Enum):
    TIMESTAMP = "timestamp"
    STRING = "string"
    LIST = "list"