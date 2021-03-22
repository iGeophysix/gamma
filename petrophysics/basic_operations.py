import numpy as np


def get_basic_stats(log_data: np.array) -> dict:
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
