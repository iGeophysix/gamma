import numpy as np


def normalize(data: dict, min_value: float = 0, max_value: float = 100) -> dict:
    '''
    This function applies linear normalization to the whole curve. NaN values remain NaN
    :param data:
    :param min_value:
    :param max_value:
    :return:
    '''

    return {md: (val - min_value) / (max_value - min_value) if not np.isnan(val) else np.nan for md, val in data.items()}
