import logging
import time
from functools import wraps

import numpy as np

logging.basicConfig()
debug = logging.getLogger("utilities")
debug.setLevel(logging.INFO)


def my_timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        task_time = int((time.time() - start) * 1000)
        debug.info(f"{func.__name__} took {task_time} ms")
        return result

    return wrapper


def safe_run(func):
    @wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError:
            return None

    return wrapped
