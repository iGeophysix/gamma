import numpy as np
import pandas as pd

from components.domain.Log import BasicLog
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


def splice_logs(well: Well, dataset_names: list[str] = None, log_names: list[str] = None) -> tuple[dict[str, np.ndarray], dict[str, dict]]:
    """
    This function processes the well and generates a dataset with spliced logs and its meta information
    :param well: Well object to process
    :param dataset_names: Optional. List of str with dataset names. If None then uses all datasets
    :param log_names: Optional. List of str with logs names. Logs must present in all datasets. If None then uses all logs in all datasets
    :return: dict with log data and log meta
    """
    if dataset_names is None:
        dataset_names = well.datasets
    logs = {}
    for dataset_name in dataset_names:
        wd = WellDataset(well, dataset_name)
        logs_in_dataset = wd.log_list if log_names is None else log_names
        logs.update({(dataset_name, log_name): BasicLog(wd.id, log_name) for log_name in logs_in_dataset})

    # here and after each log family must be spliced by families and then runs
    results_data = {}
    results_meta = {}

    for log_family in LOG_FAMILY_PRIORITY:
        # select log_names of defined family
        logs_in_family = [l for l in logs.values() if l.meta['log_family'] == log_family]
        # if no log_names in of this family - skip
        if not logs_in_family:
            continue
        # splice log_names
        spliced = splice_logs_in_family(logs_in_family)
        # define meta information
        meta = get_basic_curve_statistics(spliced)
        meta['AutoSpliced'] = {'Intervals': len(logs_in_family), 'Uncertainty': 0.5}
        meta['log_family'] = log_family
        results_data.update({log_family: spliced}) # Log name will be defined here
        results_meta.update({log_family: meta}) # log name will be defined here

    return results_data, results_meta


def splice_logs_in_family(logs: list) -> np.ndarray:
    '''
    This function takes all log_names defined in log_names meta and splices them into one
    Docs: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/f/Logs-Splicing-45J9iu3UhOtI
    :param well: Well object to work on
    :param logs_meta: dictionary with complex key {(datasetname, logname):{...meta...}, ...}
    :return: spliced log as np.ndarray
    '''

    # define smallest depth sampling rate
    step = np.min([log.meta['basic_statistics']['avg_step'] for log in logs])

    # order log_names by runs
    logs_order = [log.name for log in sorted(logs, key=lambda x: x.meta['run']['value'], reverse=True)]

    # define top and bottom of the spliced log
    min_depth = np.min([log.meta['basic_statistics']['min_depth'] for log in logs])
    max_depth = np.max([log.meta['basic_statistics']['max_depth'] for log in logs])
    new_md = np.arange(min_depth, max_depth, step)

    # interpolate logs
    logs_data_interpolated = {log.name: np.interp(new_md, log.values[:, 0], log.values[:, 1]) for log in logs}
    # splice log_names
    df = pd.DataFrame(logs_data_interpolated)
    result = df[logs_order].bfill(axis=1).iloc[:, 0]

    return np.vstack((new_md, result)).T
