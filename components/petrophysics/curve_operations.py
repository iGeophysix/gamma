import warnings

import logging
import numpy as np
from scipy import signal

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode


def geo_mean(iterable):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        a = np.log(iterable)
        return np.exp(a.mean())


def get_basic_curve_statistics(log_data: np.array) -> dict:
    '''
    Returns basic stats for a log (np.array)
    :param log_data: input data as np.array
    :return: dict with basic stats (interval of not NaN values, mean, gmean, etc)
    '''
    non_null_values = log_data[~np.isnan(log_data[:, 1])]
    min_depth = np.min(non_null_values[:, 0])
    max_depth = np.max(non_null_values[:, 0])
    min_value = np.min(non_null_values[:, 1])
    max_value = np.max(non_null_values[:, 1])
    mean = np.mean(non_null_values[:, 1])
    log_gmean = geo_mean(non_null_values[:, 1])
    stdev = np.std(non_null_values[:, 1])
    derivative = np.diff(log_data[:, 0])
    const_step = bool(abs(derivative.min() - derivative.max()) < 0.00001)
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
    '''
    This function applies linear normalization to the whole curve. NaN values remain NaN
    :param data:
    :param min_value:
    :param max_value:
    :return:
    '''

    inv_range = 1.0 / (max_value - min_value)
    offset = min_value * inv_range

    data[:, 0] = data[:, 0] * inv_range - offset

    return data


def interpolate_log_by_depth(log_data: np.array, depth_step: float) -> np.array:
    """
    This method interpolates log to bring it to a regular depth_step
    It will keep missing values inside data interval and cut out all nan values before the first not nan and after the last not nan values
    :param log_data: np.array - log data in shape ((md, val), (md,val),...)
    :param depth_step: float - new depth step
    :return np.array with resampled data
    """
    non_null_values = log_data[~np.isnan(log_data[:, 1])]
    min_depth = np.min(non_null_values[:, 0])
    max_depth = np.max(non_null_values[:, 0])
    new_depths = np.linspace(start=min_depth, stop=max_depth, num=int((max_depth - min_depth) / depth_step) + 1)
    new_values = np.interp(new_depths, log_data[:, 0], log_data[:, 1], left=np.nan, right=np.nan)
    return np.vstack((new_depths, new_values)).T


def get_log_resolution(log_data: np.array, log_meta: dict, window: float = 20) -> float:
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

    window_in_samples = np.round(window / step)
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
    logger = logging.getLogger("LogResolutionNode")
    logger.setLevel(logging.INFO)

    def _validate(self, log: BasicLog):
        '''
        Validate input data
        :param log:
        :return:
        '''
        assert abs(log.meta.basic_statistics['min_depth'] - log.meta.basic_statistics['max_depth']) > 50, 'Log is too short'

    def run(self):
        p = Project()
        well_names = p.list_wells()
        for well_name in well_names:
            well = Well(well_name)
            for dataset_name in well.datasets:
                dataset = WellDataset(well, dataset_name)
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    try:
                        self._validate(log)
                    except Exception as exc:
                        self.logger.info(f'Cannot calculate resolution on {well.name}-{dataset.name}-{log.name}. {repr(exc)}')
                        continue
                    log.meta.log_resolution = {'value': get_log_resolution(log.values, log.meta)}
                    log.save()


class BasicStatisticsNode(EngineNode):
    """
    Engine node that calculates log resolution
    """

    def run(self):
        p = Project()
        well_names = p.list_wells()
        for well_name in well_names:
            well = Well(well_name)
            for dataset_name in well.datasets:
                dataset = WellDataset(well, dataset_name)
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    log.meta.basic_statistics = get_basic_curve_statistics(log.values)
                    log.save()
