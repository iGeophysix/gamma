
import os
from datetime import datetime

import numpy as np
from celery import Celery

from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.database.settings import REDIS_HOST

from petrophysics.basic_operations import get_basic_stats
from petrophysics.petrophysics import normalize

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
    data = dataset.get_log_data(logs=list(logs.keys()))
    meta = dataset.get_log_meta(logs=list(logs.keys()))
    normalized_data = {}
    normalized_meta = {}
    for curve, params in logs.items():
        normalized_data[params["output"]] = normalize(data[curve],
                                                      params["min_value"],
                                                      params["max_value"])
        normalized_meta[params["output"]] = meta[curve]
        normalized_meta[params["output"]]["__history"].append(
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), f"Normalized curve derived from {wellname}->{datasetname}->{curve}"))

    dataset.set_data(normalized_data, normalized_meta)


@app.task
def async_get_basic_log_stats(wellname: str, datasetnames: list[str] = None, logs: list[str] = None) -> None:
    """
    Info
    Returns nothing. All results (RUN ids and basic_stats) are stored in each log meta info.
    :param wellname: well name as string
    :param datasetnames: list of dataset names to process. If None then use all datasets for the well
    :param depth_tolerance: distance to consider as acceptable difference in depth in one run
    """
    w = Well(wellname)
    if datasetnames is None:
        datasetnames = w.datasets

    # get all data from specified well and datasets
    for datasetname in datasetnames:
        d = WellDataset(w, datasetname)
        log_data = d.get_log_data(logs)
        for log, values in log_data.items():
            new_meta = get_basic_stats(values)
            d.append_log_meta({log: new_meta})



@app.task
def async_split_by_runs(wellname: str, datasetnames: list[str] = None, depth_tolerance: float = 50) -> None:
    """
    Assign RUN id_s to all logs in the well within specified datasets.
    Returns nothing. All results (RUN ids and basic_stats) are stored in each log meta info.
    :param wellname: well name as string
    :param datasetnames: list of dataset names to process. If None then use all datasets for the well
    :param depth_tolerance: distance to consider as acceptable difference in depth in one run
    """
    w = Well(wellname)
    if datasetnames is None:
        datasetnames = w.datasets

    # gather all metadata in one dictionary
    metadata = {}
    for datasetname in datasetnames:
        d = WellDataset(w, datasetname)
        metadata.update({(datasetname, log): meta for log, meta in d.get_log_meta().items()})

    # for each log curve find those that are defined at similar depth (split by runs)
    log_list = sorted(metadata.keys(), key=lambda x: metadata[x]['min_depth'], reverse=True)
    groups = {}
    group_id = 0
    while len(log_list):
        log = log_list.pop(0)
        groups.update({group_id: [log]})
        min_depth = metadata[log]['min_depth']
        max_depth = metadata[log]['max_depth']
        for other_log in log_list:
            other_log_min_depth = metadata[other_log]['min_depth']
            other_log_max_depth = metadata[other_log]['max_depth']
            if np.abs(other_log_min_depth - min_depth) < depth_tolerance and np.abs(other_log_max_depth - max_depth) < depth_tolerance:
                groups[group_id].append(other_log)
                log_list.remove(other_log)
        group_id += 1

    # sort runs
    runs = sorted(groups.values(), key=lambda x: len(x), reverse=True)

    # record run info to each log meta
    for run in runs:
        run_len = len(run)
        # get min and max depth of the run
        min_depth = metadata[run[0]]['min_depth']
        max_depth = metadata[run[0]]['max_depth']
        for datasetname, log in run:
            d = WellDataset(well=w, name=datasetname)
            d.append_log_meta({log: {"RUN": f"{run_len}_({min_depth}_{max_depth})"}})
