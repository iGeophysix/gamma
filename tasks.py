import json
from datetime import datetime

from celery import Celery
import os
from utilities import my_timer
from well import WellDataset, Well
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
app = Celery('tasks', broker=f'redis://{REDIS_HOST}')


@app.task
@my_timer
def async_read_las(wellname: str, datasetname: str, filename: str, logs: dict):
    types_conversion = {
        'float': float,
        'str': str,
        'int': int,
        'datetime': datetime
    }

    well = Well(wellname)
    dataset = WellDataset(well, datasetname)
    _logs = json.loads(logs)
    l = {k: types_conversion.get(v, str) for k, v in _logs.items()}
    dataset.read_las(filename, l)
