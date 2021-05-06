import logging
from collections import defaultdict
from typing import Iterable

import numpy as np

from celery_conf import app as celery_app, check_task_completed
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode


class AlgorithmFailure(Exception):
    pass


def get_best_log(datasets: Iterable[WellDataset], family: str, run_name: str) -> tuple[str, dict]:
    """
    This method defines best in family log in a dataset withing one run
    :param datasets: list of WellDataset objects to process
    :param family: log family to process e.g. 'Gamma Ray'
    :param run_name: run_id to process e.g. '75_(2600_2800)'
    :return: tuple with name of the best log and dict of new log meta
    """

    # gather all raw logs to process
    logs_meta = {}
    for ds in datasets:
        for log_id in ds.log_list:
            log = BasicLog(ds.id, log_id)
            if log.meta.family == family and log.meta.run['value'] == run_name and 'raw' in log.meta.tags:
                logs_meta.update({log_id: log.meta})

    if not logs_meta:
        raise AlgorithmFailure('No logs of this family in this run')

    best_log, new_logs_meta = rank_logs(logs_meta)

    return best_log, new_logs_meta


def rank_logs(logs_meta):
    averages = {('basic_statistics', "mean"): None, ('basic_statistics', "stdev"): None, ('log_resolution', 'value'): None}
    for category, metric in averages.keys():
        averages[(category, metric)] = np.median([logs_meta[log_id][category][metric] for log_id in logs_meta.keys()])
    average_depth_correction = np.max([logs_meta[log_id]['basic_statistics']['max_depth'] - logs_meta[log_id]['basic_statistics']['min_depth'] for log_id in logs_meta.keys()])
    best_score = np.inf
    best_log = None
    new_logs_meta = {log_id: {} for log_id in logs_meta.keys()}
    for log_id in logs_meta.keys():
        sum_deltas = sum(abs((logs_meta[log_id][category][metric] - averages[(category, metric)]) / averages[(category, metric)]) for category, metric in averages.keys())
        depth_correction = abs((logs_meta[log_id]['basic_statistics']['max_depth'] - logs_meta[log_id]['basic_statistics']['min_depth']) / average_depth_correction)
        result = sum_deltas / depth_correction
        if result < best_score:
            best_log = log_id
            best_score = result
        new_logs_meta[log_id].update({'best_log_detection': {"value": result, 'is_best': False}})
    if best_log is None:
        raise AlgorithmFailure('No logs were defined as best')
    # define ranking
    ranking = sorted(new_logs_meta.items(), key=lambda v: v[1]['best_log_detection']['value'])
    for i, meta in enumerate(ranking):
        new_logs_meta[meta[0]]['best_log_detection']['rank'] = i + 1
    new_logs_meta[best_log]['best_log_detection']['is_best'] = True
    return best_log, new_logs_meta


class BestLogDetectionNode(EngineNode):
    logger = logging.getLogger("BestLogDetectionNode")
    logger.setLevel(logging.INFO)

    def run(self):
        p = Project()
        tree = p.tree_oop()
        tasks = []
        for well, datasets in tree.items():
            runs = defaultdict(lambda: defaultdict(list))
            for dataset, logs in datasets.items():
                # gather runs in dataset
                for log in logs:
                    if not 'raw' in log.meta.tags:
                        continue
                    runs[log.meta.run['value']][log.meta.family].append(log)

            for run, families in runs.items():
                for family, logs in families.items():
                    logs_paths = [(log.dataset_id, log._id) for log in logs]
                    tasks.append(celery_app.send_task('tasks.async_detect_best_log', (logs_paths,)))

        while not all(map(check_task_completed, tasks)):
            continue


