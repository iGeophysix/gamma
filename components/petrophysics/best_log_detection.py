import itertools
import logging
from typing import Union, Iterable, Optional

import numpy as np

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode


class AlgorithmFailure(Exception):
    pass


def get_best_log(dataset: WellDataset, family: str, run_name: str) -> tuple[str, dict]:
    """
    This method defines best in family log in a dataset withing one run
    :param dataset: WellDataset object to process
    :param family: log family to process e.g. 'Gamma Ray'
    :param run_name: run_id to process e.g. '75_(2600_2800)'
    :return: tuple with name of the best log and dict of new log meta
    """
    log_ids = dataset.log_list
    logs = [BasicLog(dataset.id, log_id) for log_id in log_ids]

    logs_meta = {log_id: log.meta for log_id, log in zip(log_ids, logs) if log.meta.family == family and log.meta.run['value'] == run_name}

    averages = {('basic_statistics', "mean"): None, ('basic_statistics', "stdev"): None, ('log_resolution', 'value'): None}
    for category, metric in averages.keys():
        averages[(category, metric)] = np.median([logs_meta[log_id][category][metric] for log_id in logs_meta.keys()])
    average_depth_correction = np.max([logs_meta[log_id]['basic_statistics']['max_depth'] - logs_meta[log_id]['basic_statistics']['min_depth'] for log_id in logs_meta.keys()])

    best_score = np.inf
    best_log = None
    new_logs_meta = {log_id: {} for log_id in log_ids}
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
    new_logs_meta[best_log]['best_log_detection']['is_best'] = True

    return best_log, new_logs_meta


class BestLogDetectionNode(EngineNode):
    logger = logging.getLogger("BestLogDetectionNode")
    logger.setLevel(logging.INFO)

    def run(self):
        p = Project()
        well_names = p.list_wells()
        for well_name in well_names:
            well = Well(well_name)
            dataset_names = well.datasets
            for dataset_name in dataset_names:
                dataset = WellDataset(well, dataset_name)

                # gather runs in dataset
                runs = set()
                log_families = set()
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    runs.update((log.meta.run['value'],))
                    log_families.update((log.meta.family,))

                for run, family in itertools.product(runs, log_families):
                    try:
                        best_log, new_meta = get_best_log(dataset=dataset, family=family, run_name=run)

                        for log_id, values in new_meta.items():
                            l = BasicLog(dataset.id, log_id)
                            l.meta.update(values)
                            l.save()
                    except AlgorithmFailure as exc:
                        logging.info(f"Well {well.name} dataset {dataset.name} family {family} run {run}. {repr(exc)}")
                        continue


def family_best_log(well_name: str, log_family: Union[str, Iterable[str]]) -> Optional[BasicLog]:
    '''
    Finds the best log version of the specific family(ies)
    :param well_name: well to search into
    :param log_family: name of a log family or list of acceptable family variants
    :return: best log or None
    '''
    wanted_families = (log_family,) if isinstance(log_family, str) else set(log_family)
    best_log = None
    well = Well(well_name)
    for dataset_name in well.datasets:
        dataset = WellDataset(well, dataset_name)
        for log_id in dataset.log_list:
            log = BasicLog(dataset.id, log_id)
            if hasattr(log.meta, 'family') and log.meta.family in wanted_families:
                # best variant
                if hasattr(log.meta, 'best_log_detection') and log.meta.best_log_detection['is_best']:
                    return log
                # just an acceptable family
                elif best_log is None:
                    best_log = log
    return best_log
