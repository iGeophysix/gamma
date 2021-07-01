import logging
import time
from functools import wraps

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


def dict_replace_value(d, old, new):
    x = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict_replace_value(v, old, new)
        elif isinstance(v, list):
            v = list_replace_value(v, old, new)
        elif isinstance(v, str):
            v = v.replace(str(old), str(new))
        elif v != v:
            v = None
        x[k] = v
    return x


def list_replace_value(l, old, new):
    x = []
    for e in l:
        if isinstance(e, list):
            e = list_replace_value(e, old, new)
        elif isinstance(e, dict):
            e = dict_replace_value(e, old, new)
        elif isinstance(e, str):
            e = e.replace(str(old), str(new))
        elif e != e:
            e = None
        x.append(e)
    return x
