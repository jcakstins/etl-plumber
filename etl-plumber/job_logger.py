import logging
import sys
import traceback
from logging import Logger

logger: Logger = logging.getLogger()
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(levelname)s:%(module)s.%(name)s.%(funcName)s: %(message)s')

def exception_logger(exception_msg=None, exception_cls: Exception = Exception):
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exception_cls as err:
                logger.error(f"{exception_msg}\n{str(err)}")
        return wrapper
    return decorator

def exception_traceback(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            traceback.print_exc()
    return wrapper