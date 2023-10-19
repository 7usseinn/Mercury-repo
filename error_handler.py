import logging
from datetime import datetime

logging.basicConfig(filename='my_log.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger()

def log_message(level, message):
    if level == 'error':
        logger.error(message)
    elif level == 'warning':
        logger.warning(message)
    elif level == 'info':
        logger.info(message)
    else:
        logger.debug(message)

def print_error(suffix,prefix,level,message):
    log_message(level,message)
    print(f"{datetime.now()} - {level} - {message} - {suffix} - {prefix}")
