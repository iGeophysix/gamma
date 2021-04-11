import numpy as np
import pandas as pd

from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.curve_operations import get_basic_curve_statistics

LOG_FAMILY_PRIORITY = [
    'Gamma Ray',
    'Bulk Density',
    'Neutron Porosity',
    'Resistivity',
    'Formation Resistivity',
    'Compressional Slowness',
]


def splice_logs_in_family(well: Well, logs_meta: dict) -> np.ndarray:
    '''
    This function takes all logs defined in logs meta and splices them into one
    Docs: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/f/Logs-Splicing-45J9iu3UhOtI
    :param well: Well object to work on
    :param logs_meta: dictionary with complex key {(datasetname, logname):{...meta...}, ...}
    :return: spliced log as np.ndarray
    '''

    def get_data(dataset_log):
        dataset_name, log_name = dataset_log
        wd = WellDataset(well, dataset_name)
        return wd.get_log_data(logs=[log_name, ])[log_name]

    # define smallest depth sampling rate
    step = np.min([meta['basic_statistics']['avg_step'] for meta in logs_meta.values()])

    # order logs by runs
    logs_order = [log for log, _ in sorted(logs_meta.items(), key=lambda x: x[1]['Run_AutoCalculated'], reverse=True)]
    # get logs data
    logs_data = {log: get_data(log) for log in logs_meta.keys()}
    # define top and bottom of the spliced log
    min_depth = np.min([meta['basic_statistics']['min_depth'] for meta in logs_meta.values()])
    max_depth = np.max([meta['basic_statistics']['max_depth'] for meta in logs_meta.values()])
    new_md = np.arange(min_depth, max_depth, step)

    # interpolate logs
    logs_data_interpolated = {log: np.interp(new_md, values[:, 0], values[:, 1]) for log, values in logs_data.items()}
    # splice logs
    df = pd.DataFrame(logs_data_interpolated)
    result = df[logs_order].bfill(axis=1).iloc[:, 0]

    return np.vstack((new_md, result)).T


def splice_logs(well: Well, dataset_names: list[str] = None, logs: list[WellDataset] = None) -> tuple[dict[str, np.ndarray], dict[str, dict]]:
    """
    This function processes the well and generates a dataset with spliced logs and its meta information
    :param well: Well object to process
    :param dataset_names: Optional. List of str with dataset names. If None then uses all datasets
    :param logs: Optional. List of str with logs names. Logs must present in all datasets. If None then uses all logs in all datasets
    :return: dict with log data and log meta
    """
    if dataset_names is None:
        dataset_names = well.datasets
    all_meta = {}
    for dataset_name in dataset_names:
        wd = WellDataset(well, dataset_name)
        logs_in_dataset = wd.get_log_list() if logs is None else logs
        all_meta.update({(dataset_name, log_name): meta for log_name, meta in wd.get_log_meta(logs=logs_in_dataset).items()})

    # here and after each log family must be spliced by families and then runs
    results_data = {}
    results_meta = {}

    for log_family in LOG_FAMILY_PRIORITY:
        # select logs of defined family
        logs_in_family = {log: meta for log, meta in all_meta.items() if meta['log_family'] == log_family}
        # if no logs in of this family - skip
        if not logs_in_family:
            continue
        # splice logs
        spliced = splice_logs_in_family(well, logs_in_family)
        # define meta information
        meta = get_basic_curve_statistics(spliced)
        meta['AutoSpliced'] = {'Intervals': len(logs_in_family), 'Uncertainty': 0.5}
        results_data.update({log_family: spliced})
        results_meta.update({log_family: meta})

    return results_data, results_meta
