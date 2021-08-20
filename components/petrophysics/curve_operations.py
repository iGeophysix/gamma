import logging
import warnings
from datetime import datetime
from typing import Tuple, List, Iterable
import copy

import numpy as np
from scipy import signal

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog, BasicLogMeta
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from settings import LOGGING_LEVEL


def geo_mean(iterable):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        a = np.log(iterable)
        return np.exp(a.mean())


def get_basic_curve_statistics(log_data: np.array) -> dict:
    """
    Returns basic stats for a log (np.array)
    :param log_data: input data as np.array
    :return: dict with basic stats (interval of not NaN values, mean, gmean, etc)
    """
    non_null_values = log_data[~np.isnan(log_data[:, 1])]
    min_depth = np.min(non_null_values[:, 0])
    max_depth = np.max(non_null_values[:, 0])
    min_value = np.min(non_null_values[:, 1])
    max_value = np.max(non_null_values[:, 1])
    mean = np.mean(non_null_values[:, 1])
    log_gmean = geo_mean(non_null_values[:, 1])
    stdev = np.std(non_null_values[:, 1])
    derivative = np.diff(log_data[:, 0])
    const_step = bool(abs(derivative.min() - derivative.max()) < 0.001)
    avg_step = derivative.mean()
    new_meta = {"min_depth": min_depth,
                "max_depth": max_depth,
                "min_value": min_value,
                "max_value": max_value,
                "depth_span": max_depth - min_depth,
                "avg_step": avg_step,
                "const_step": const_step,
                "mean": mean,
                "gmean": log_gmean,
                "stdev": stdev}
    return new_meta


def rescale_curve(data, min_value: float = 0, max_value: float = 100):
    """
    This function applies linear normalization to the whole curve. NaN values remain NaN
    :param data:
    :param min_value:
    :param max_value:
    :return:
    """

    inv_range = 1.0 / (max_value - min_value)
    offset = min_value * inv_range

    data[:, 0] = data[:, 0] * inv_range - offset

    return data


def interpolate_log_by_depth(log_data: np.array,
                             depth_step: float = None,
                             depth_start: float = None,
                             depth_stop: float = None,
                             depth_to: np.array = None) -> np.array:
    """
    This method interpolates log to bring it to a new depth.
    Specify either depth_to only or depth_step plus optional depth_start and depth_stop.
    It will keep missing values inside data interval and cut out
    all nan values before the first not nan and after the last not nan values.

    :param log_data: np.array - log data in shape ((md, val), (md, val),...)
    :param depth_step: float - new reference depth step
    :param depth_start: float - first depth of the new reference
    :param depth_stop: float - last depth of the new reference
    :param depth_to: np.array - new reference
    :return np.array with resampled data in shape ((md, val), (md, val),...)
    """

    non_null_values = log_data[~np.isnan(log_data[:, 1])]
    if depth_to is None:
        assert depth_step is not None, ValueError('Either depth_to or depth_step is mandatory')
        if depth_start is None:
            depth_start = np.min(non_null_values[:, 0])
        if depth_stop is None:
            depth_stop = np.max(non_null_values[:, 0])
        depth_to = np.linspace(start=depth_start, stop=depth_stop, num=int((depth_stop - depth_start) / depth_step) + 1)
    new_values = np.interp(depth_to, log_data[:, 0], log_data[:, 1], left=np.nan, right=np.nan)
    return np.vstack((depth_to, new_values)).T


def interpolate_to_common_reference(logs: Iterable[BasicLog]) -> List[BasicLog]:
    '''
    Interpolates set of logs to a common depths
    :param logs: list of input logs
    :return: list of interpolated logs with a common reference
    '''
    step = np.inf
    min_depth = np.inf
    max_depth = -np.inf

    for log in logs:
        if not log.empty:
            bs = log.meta['basic_statistics']
            # define smallest depth sampling rate
            step = min(step, bs['avg_step'])
            # define top and bottom of the common reference
            min_depth = min(min_depth, bs['min_depth'])
            max_depth = max(max_depth, bs['max_depth'])
    all_empty = step == np.inf

    # new common reference
    if all_empty:
        depth_to = np.empty(0)
    else:
        depth_to = np.linspace(start=min_depth, stop=max_depth, num=int((max_depth - min_depth) / step) + 1)

    res_logs = []
    for log in logs:
        int_log = copy.copy(log)
        if all_empty:
            int_log.values = np.empty((0, 2))
        else:
            int_log.values = interpolate_log_by_depth(log.values,
                                                      depth_to=depth_to)
        res_logs.append(int_log)
    return res_logs


def get_log_resolution(log_data: np.array, log_meta: BasicLogMeta, window: float = 20) -> float:
    """
    Returns basic stats for a log (np.array)
    :param log_data: input data as np.array
    :param log_meta: input metadata as dict
    :param window : float Window length in log depth reference. Must be smaller than log_data->depth_span
    :return log_resolution : float number describing resolution of the log
    """
    step = log_meta.basic_statistics['avg_step']
    if not log_meta.basic_statistics['const_step']:
        log_data = interpolate_log_by_depth(log_data, step)

    window_in_samples = np.round(window / abs(step))
    if window_in_samples == 0 or step > 1:  # if depth step is bigger than window or if avg step is > 1 then resolution is too low and we assume it equals np.nan
        return np.nan

    gauss_std = (window_in_samples - 1) / 6
    gauss_window = signal.windows.gaussian(window_in_samples, gauss_std, sym=True)
    gauss_window /= gauss_window.sum()
    smoothed = np.convolve(log_data[:, 1], gauss_window, mode='same')
    crop = int(window_in_samples / 2)
    log_resolution = np.nanmean(np.abs(log_data[crop:-crop, 1] - smoothed[crop:-crop]))
    return log_resolution


class LogResolutionNode(EngineNode):
    """
    Engine node that calculates log resolution
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(LOGGING_LEVEL)

    @classmethod
    def name(cls):
        """Return node name"""
        return cls.__name__

    @classmethod
    def version(cls):
        """Return node version"""
        return 1

    @staticmethod
    def validate(log: BasicLog):
        """
        Validate input data
        :param log:
        :return:
        """
        if 'raw' not in log.meta.tags:
            raise TypeError('Not raw data')
        if 'main_depth' in log.meta.tags:
            raise TypeError('Depth data')
        assert abs(log.meta.basic_statistics['min_depth'] - log.meta.basic_statistics['max_depth']) > 50, 'Log is too short'

    @classmethod
    def run_for_item(cls, **kwargs):
        """
        Calculate Log Resolution for each log
        :param kwargs: dataset_id, log_id
        :return:
        """
        dataset_id = kwargs['dataset_id']
        log_id = kwargs['log_id']

        log = BasicLog(dataset_id, log_id)
        if not log.empty:
            log_resolution = get_log_resolution(log.values, log.meta)
            log.meta.log_resolution = {'value': log_resolution}
            cls.write_history(log=log)
            log.save()

    @classmethod
    def item_hash(cls, log: BasicLog) -> Tuple[str, bool]:
        """Get item hash to use in cache"""

        return log.data_hash, hasattr(log.meta, 'log_resolution')

    @classmethod
    def run(cls, **kwargs):
        """
        Run Log resolution node
        :param kwargs:
        :return:
        """
        p = Project()
        well_names = p.list_wells()
        tasks = []
        hashes = []
        cache_hits = 0
        cache = EngineNodeCache(cls)
        for well_name in well_names:
            well = Well(well_name)
            for dataset_name in well.datasets:
                if dataset_name == 'LQC':
                    continue

                dataset = WellDataset(well, dataset_name)
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    item_hash, valid = cls.item_hash(log)
                    if valid and item_hash in cache:
                        hashes.append(item_hash)
                        cache_hits += 1
                        continue

                    try:
                        cls.validate(log)
                    except TypeError as exc:
                        error_message = f'Cannot calculate resolution on {well.name}-{dataset.name}-{log.name}. {repr(exc)}'
                        cls.logger.debug(error_message)
                        continue
                    except Exception as exc:
                        error_message = f'Cannot calculate resolution on {well.name}-{dataset.name}-{log.name}. {repr(exc)}'
                        cls.logger.info(error_message)
                        log.meta.add_tags('no_resolution', 'bad_quality')
                        log.save()
                        continue

                    result = celery_app.send_task('tasks.async_log_resolution', (dataset.id, log_id,))
                    tasks.append(result)
                    hashes.append(item_hash)

        cache.set(hashes)
        cls.logger.info(f'Node: {cls.name()}: cache hits:{cache_hits} / misses: {len(tasks)}')

        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def write_history(cls, **kwargs):
        """Write node history in logs' meta """
        kwargs['log'].meta.append_history({'node': cls.name(),
                                           'node_version': cls.version(),
                                           'timestamp': datetime.now().isoformat(),
                                           'parent_logs': [],
                                           'parameters': {}
                                           })


class BasicStatisticsNode(EngineNode):
    """
    Engine node that calculates log basic statistics.
    """

    @classmethod
    def version(cls):
        return 0

    @classmethod
    def run_for_item(cls, **kwargs):
        """
        Calculates basic log statics for all given lognames
        in the given well and dataset.
        """
        wellname = kwargs['wellname']
        datasetnames = kwargs['datasetnames']
        lognames = kwargs['lognames']

        w = Well(wellname)
        if datasetnames is None:
            datasetnames = w.datasets

        # get all data from specified well and datasets
        for datasetname in datasetnames:
            d = WellDataset(w, datasetname)

            loop_lognames = d.log_list if lognames is None else lognames

            for log_name in loop_lognames:
                log = BasicLog(d.id, log_name)
                log.meta.update({'basic_statistics': get_basic_curve_statistics(log.values)})
                cls.write_history(log=log)
                log.save()

    @classmethod
    def run(cls):
        p = Project()
        well_names = p.list_wells()
        tasks = []
        for well_name in well_names:
            well = Well(well_name)
            for dataset_name in well.datasets:
                dataset = WellDataset(well, dataset_name)
                for log_name in dataset.log_list:
                    result = celery_app.send_task('tasks.async_get_basic_log_stats',
                                                  (well_name, [dataset_name, ], [log_name, ]))
                    tasks.append(result)
        wait_till_completes(tasks)

    @classmethod
    def write_history(cls, **kwargs):
        kwargs['log'].meta.append_history({'node': cls.name(),
                                           'node_version': cls.version(),
                                           'timestamp': datetime.now().isoformat(),
                                           'parent_logs': [],
                                           'parameters': {}
                                           })
