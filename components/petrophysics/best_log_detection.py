import logging
from collections import defaultdict
from typing import Iterable

import numpy as np

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode

LOG_TAG_ASSESSMENT = {
    'average': -5,
    'azimuthal': -2,
    'bad_quality': -5,
    'best': 3,
    'calibrated': 2,
    'compensated': 1,
    'computed': -5,
    'conventional': 1,
    'corrected': 1,
    'delayed': -1,
    'enhanced': 1,
    'filtered': -1,
    'focused': 2,
    'high resolution': 2,
    'horizontal': -1,
    'image': -5,
    'memory': 2,
    'natural': 0,
    'normalized': 1,
    'ratio': -4,
    'raw': 0,
    'real-time': -2,
    'reconstructed': -4,
    'synthetic': -4,
    'smoothed': -3,
    'station log': -5,
    'theoretical': -5,
    'transmitted': 0,
    'true': 2,
    'uncorrected': -1,
    'vertical': 0
}


def score_log_tags(tags: Iterable[str]) -> int:
    '''
    Calculates log usefulness by log tags
    :param tags: set of log tags
    :return: log usefulness score
    '''
    tags = set(map(str.lower, tags))    # remove duplicates, case-insensitive
    rank = sum(LOG_TAG_ASSESSMENT.get(tag, 0) for tag in tags)
    return rank


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
        val = np.median([logs_meta[log_id][category][metric] for log_id in logs_meta.keys()])
        # little trick to avoid division by zero : https://gammalog.jetbrains.space/im/review/2vqWfb3Gfp0m?message=1Jv0001Jv&channel=2pl5Dd4IH2oq
        averages[(category, metric)] = val if val != 0 else val + 1e-16

    average_depth_correction = np.max([logs_meta[log_id]['basic_statistics']['max_depth'] - logs_meta[log_id]['basic_statistics']['min_depth'] for log_id in logs_meta.keys()])
    # little trick to avoid division by zero : https://gammalog.jetbrains.space/im/review/2vqWfb3Gfp0m?message=1Jv0001Jv&channel=2pl5Dd4IH2oq
    average_depth_correction = average_depth_correction if average_depth_correction != 0 else average_depth_correction + 1e-16

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

    @staticmethod
    def validate(log: BasicLog) -> bool:
        return 'raw' in log.meta.tags \
               and 'bad_quality' not in log.meta.tags \
               and hasattr(log.meta, 'family') \
               and hasattr(log.meta, 'basic_statistics') \
               and hasattr(log.meta, 'log_resolution')

    @classmethod
    def run(cls):
        p = Project()
        tree = p.tree_oop()
        tasks = []
        for well, datasets in tree.items():
            runs = defaultdict(lambda: defaultdict(list))
            for dataset, logs in datasets.items():
                if dataset.name == 'LQC':
                    continue
                # gather runs in dataset
                for log in logs:
                    if cls.validate(log):
                        runs[log.meta.run['value']][log.meta.family].append(log)

            for run, families in runs.items():
                for family, logs in families.items():
                    logs_paths = [(log.dataset_id, log._id) for log in logs]
                    tasks.append(celery_app.send_task('tasks.async_detect_best_log', (logs_paths,)))

        wait_till_completes(tasks)
