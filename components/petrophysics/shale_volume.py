import numpy as np

from components.domain.Log import BasicLog
from components.petrophysics.curve_operations import get_basic_curve_statistics


def _linear_scale(arr, lower_limit, upper_limit):
    inv_range = 1 / (upper_limit - lower_limit)
    offset = lower_limit * inv_range

    result = arr * inv_range - offset
    return result


def linear_method(log: BasicLog, gr_matrix: float, gr_shale: float) -> BasicLog:
    """
    Function to calculate Shale Volume (VSH) via linear method
    :param log: BasicLog
    :param gr_matrix: float, Gamma Ray value at matrix
    :param gr_shale: float, Gamma Ray value at shale
    :return: BasicLog (virtual)
    """
    vsh = BasicLog(id='VSH_GR_LM')

    vsh.meta = log.meta
    vsh.log_family = "Shale Volume"
    vsh.meta = vsh.meta | {"method": "Linear method based on Gamma Ray logs"}

    values = log.values
    values[:, 1] = np.clip(_linear_scale(values[:, 1], gr_matrix, gr_shale), 0, 1)
    vsh.values = values
    basic_stats = get_basic_curve_statistics(vsh.values)
    vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

    return vsh


def larionov_older_rock_method(log, gr_matrix: float, gr_shale: float) -> BasicLog:
    """
    Function to calculate Shale Volume (VSH) via Larionov older rock methods
    :param log: BasicLog
    :param gr_matrix: float, Gamma Ray value at matrix
    :param gr_shale: float, Gamma Ray value at shale
    :return: BasicLog (virtual)
    """
    vsh = BasicLog(id='VSH_GR_LOR')

    vsh.meta = log.meta
    vsh.log_family = "Shale Volume"
    vsh.meta = vsh.meta | {"method": "Larionov older rock method based on Gamma Ray logs"}

    values = log.values
    gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
    values[:, 1] = np.clip(0.33 * (2 ** (2 * gr_index) - 1), 0, 1)
    vsh.values = values
    basic_stats = get_basic_curve_statistics(vsh.values)
    vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

    return vsh


def larionov_tertiary_rock_method(log, gr_matrix: float, gr_shale: float) -> BasicLog:
    """
    Function to calculate Shale Volume (VSH) via Larionov tertiary rock methods
    :param log: BasicLog
    :param gr_matrix: float, Gamma Ray value at matrix
    :param gr_shale: float, Gamma Ray value at shale
    :return: BasicLog (virtual)
    """
    vsh = BasicLog(id='VSH_GR_LTR')

    vsh.meta = log.meta
    vsh.log_family = "Shale Volume"
    vsh.meta = vsh.meta | {"method": "Larionov tertiary rock method based on Gamma Ray logs"}

    values = log.values
    gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
    values[:, 1] = np.clip(0.083 * (2 ** (3.7 * gr_index) - 1), 0, 1)
    vsh.values = values
    basic_stats = get_basic_curve_statistics(vsh.values)
    vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

    return vsh
