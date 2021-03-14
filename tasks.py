import json
import os

from celery import Celery

from petrophysics import normalize
from well import WellDataset, Well

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
app = Celery('tasks', broker=f'redis://{REDIS_HOST}')


@app.task
def async_read_las(wellname: str, datasetname: str, filename: str):
    well = Well(wellname)
    dataset = WellDataset(well, datasetname)
    dataset.read_las(filename)


@app.task
def async_set_data(wellname: str, datasetname: str, data: frozenset):
    well = Well(wellname)
    dataset = WellDataset(well, datasetname)
    dataset.set_data(data)


@app.task
def async_normalize_log(wellname: str, datasetname: str, logs: dict) -> None:
    '''
    Apply asyncronous normalization of curves in a dataset
    :param wellname:
    :param datasetname:
    :param logs:
        {"GR": {"min_value":0,"max_value":150, "output":"GR_norm"}, "RHOB": {"min_value":1.5,"max_value":2.5, "output":"RHOB_norm"},}
    :return:
    '''

    well = Well(wellname)
    dataset = WellDataset(well, datasetname)
    data = dataset.get_log_data(logs=logs.keys())
    normalized_data = {}
    for curve, p in logs.items():
        normalized_data.update({p["output"]: json.dumps(normalize(data[curve], p["min_value"], p["max_value"]))})

    dataset.set_data(normalized_data)
