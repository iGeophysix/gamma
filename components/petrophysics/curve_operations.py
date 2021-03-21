import numpy as np


def get_basic_curve_statistics(log_data: np.array) -> dict:
    '''
    Returns basic stats for a log (np.array)
    :param log_data: input data as np.array
    :return: dict with basic stats (interval of not NaN values, mean, gmean, etc)
    '''
    non_null_values = log_data[~np.isnan(log_data[:, 1])]
    min_depth = np.min(non_null_values[:, 0])
    max_depth = np.max(non_null_values[:, 0])
    mean = np.mean(non_null_values[:, 1])
    gmean = np.exp(np.mean(np.log(non_null_values[:, 1])))
    stdev = np.std(non_null_values[:, 1])
    derivative = np.diff(log_data[:, 0])
    const_step = bool(abs(derivative.min() - derivative.max()) < 0.00001)
    avg_step = derivative.mean()
    new_meta = {"min_depth": min_depth,
                "max_depth": max_depth,
                "depth_span": max_depth - min_depth,
                "avg_step": avg_step,
                "const_step": const_step,
                "mean": mean,
                "gmean": gmean,
                "stdev": stdev}
    return new_meta


def normalize_curve(data, min_value: float = 0, max_value: float = 100):
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
