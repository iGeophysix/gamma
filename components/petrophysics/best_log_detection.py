import numpy as np

from components.domain.WellDataset import WellDataset


class AlgorithmFailure(Exception):
    pass


def get_best_log(dataset: WellDataset, log_family: str, run_name: str) -> tuple[str, dict]:
    """
    This method defines best in family log in a dataset withing one run
    :param dataset: WellDataset object to process
    :param log_family: log family to process e.g. 'Gamma Ray'
    :param run_name: run_id to process e.g. '75_(2600_2800)'
    :return: tuple with name of the best log and dict of new log meta
    """
    logs = dataset.get_log_list(log_family=log_family, Run_AutoCalculated=run_name)
    logs_meta = dataset.get_log_meta(logs=logs)

    averages = {"mean": None, "stdev": None, 'LogResolution_AutoCalculated': None}
    for avg in averages.keys():
        averages[avg] = np.median([logs_meta[log][avg] for log in logs])
    average_depth_correction = np.max([logs_meta[log]['max_depth'] - logs_meta[log]['min_depth'] for log in logs])

    best_score = np.inf
    best_log = None
    new_logs_meta = {log: {} for log in logs}
    for log in logs:
        sum_deltas = sum(abs((logs_meta[log][param] - averages[param]) / averages[param]) for param in averages.keys())
        depth_correction = abs((logs_meta[log]['max_depth'] - logs_meta[log]['min_depth']) / average_depth_correction)
        result = sum_deltas / depth_correction
        if result < best_score:
            best_log = log
            best_score = result
        new_logs_meta[log].update({'BestLog_Score_AutoCalculated': result, 'BestLog_AutoCalculated': False})

    if best_log is None:
        raise AlgorithmFailure('No logs were defined as best')
    new_logs_meta[best_log]['BestLog_AutoCalculated'] = True

    return best_log, new_logs_meta
