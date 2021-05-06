import numpy as np
import pandas as pd

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode
from components.petrophysics.curve_operations import get_basic_curve_statistics

LOG_FAMILY_PRIORITY = [
    'Gamma Ray',
    'Bulk Density',
    'Density',
    'Neutron Porosity',
    'Resistivity',
    'Formation Resistivity',
    'Compressional Slowness',
    'Thermal Neutron Porosity',
]


def splice_logs(well: Well, dataset_names: list[str] = None, log_names: list[str] = None, tags: list[str] = None) -> tuple[dict[str, np.ndarray], dict[str, dict]]:
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

    if tags is not None:
        logs = {log_id: log for log_id, log in logs.items() if any(tag in log.meta.tags for tag in tags)}

    # here and after each log family must be spliced by families and then runs
    results_data = {}
    results_meta = {}

    for family in LOG_FAMILY_PRIORITY:
        # select log_names of defined family
        logs_in_family = [l for l in logs.values() if l.meta['family'] == family]
        # if no log_names in of this family - skip
        if not logs_in_family:
            continue
        # splice log_names
        spliced = splice_logs_in_family(logs_in_family)
        # define meta information
        meta = {'basic_statistics': get_basic_curve_statistics(spliced)}
        meta['AutoSpliced'] = {'Intervals': len(logs_in_family), 'Uncertainty': 0.5}
        meta['family'] = family
        meta['units'] = logs_in_family[0].meta.units # TODO: check spliced log units are defined correctly
        results_data.update({family: spliced})  # Log name will be defined here
        results_meta.update({family: meta})  # log name will be defined here

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


class SpliceLogsNode(EngineNode):
    """
    Runs log splicing in all wells in the project
    """

    def run(self, output_dataset_name: str = "LQC"):
        p = Project()
        well_names = p.list_wells()
        tasks = []
        for well_name in well_names:
            tasks.append(celery_app.send_task('tasks.async_splice_logs', kwargs={'wellname': well_name, 'tags': ['processing', ], 'output_dataset_name': output_dataset_name}))

        wait_till_completes(tasks)
