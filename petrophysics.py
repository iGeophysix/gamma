import numpy as np


def normalize(data, min_value: float = 0, max_value: float = 100) -> dict:
    '''
    This function applies linear normalization to the whole curve. NaN values remain NaN
    :param data:
    :param min_value:
    :param max_value:
    :return:
    '''

    inv_range = 1.0 / (max_value - min_value)
    offset = min_value * inv_range

    data[:,0] = data[:,0] * inv_range  - offset

    return data
