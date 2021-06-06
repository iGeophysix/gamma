import logging

import numpy as np
import pandas as pd
import scipy.interpolate

from celery_conf import app as celery_app, wait_till_completes
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.UnitsSystem import UnitConversionError
from settings import DEFAULT_LQC_NAME

LOG_FAMILY_PRIORITY = [
    'Gamma Ray',
    'Bulk Density',
    'Density',
    'Neutron Porosity',
    'Resistivity',
    'Formation Resistivity',
    'Compressional Slowness',
    'Neutron Porosity',
]

logger = logging.getLogger('LogSplicingNode')


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

    for family, family_meta in FamilyProperties().items():
        if not family_meta.get('splice', False):
            continue

        # select log_names of defined family
        logs_in_family = [l for l in logs.values() if hasattr(l.meta, 'family') and l.meta['family'] == family]
        # if no log_names in of this family - skip
        if not logs_in_family:
            continue
        # splice log_names
        target_units = family_meta.get('unit', None)
        try:
            spliced = splice_logs_in_family(logs_in_family, target_units=target_units)
        except AttributeError as exc:
            logger.warning(str(exc) + f" Family: {family}")
            continue
        # define meta information
        meta = {}
        meta['AutoSpliced'] = {'Intervals': len(logs_in_family), 'Uncertainty': 0.5}
        meta['family'] = family
        meta['units'] = target_units
        results_data.update({family_meta.get('mnemonic', family): spliced})  # Log name will be defined here
        results_meta.update({family_meta.get('mnemonic', family): meta})  # log name will be defined here

    return results_data, results_meta


def splice_logs_in_family(logs: list, target_units: str = None) -> np.ndarray:
    '''
    This function takes all log_names defined in log_names meta and splices them into one
    Docs: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/f/Logs-Splicing-45J9iu3UhOtI
    :param well: Well object to work on
    :param logs_meta: dictionary with complex key {(datasetname, logname):{...meta...}, ...}
    :param target_units: output logs family. Default: None - no conversion
    :return: spliced log as np.ndarray
    '''

    # define smallest depth sampling rate
    step = abs(np.min([log.meta['basic_statistics']['avg_step'] for log in logs]))

    # order log_names by runs

    # define top and bottom of the spliced log
    min_depth = np.min([log.meta['basic_statistics']['min_depth'] for log in logs])
    max_depth = np.max([log.meta['basic_statistics']['max_depth'] for log in logs])
    new_md = np.arange(min_depth, max_depth + step, step)

    # interpolate logs
    logs_data_interpolated = {}
    for log in logs:
        if target_units is not None:
            try:
                log_values = log.convert_units(target_units)
            except UnitConversionError:
                continue
        interpolated_values = scipy.interpolate.interp1d(log_values[:, 0], log_values[:, 1], fill_value=np.nan, bounds_error=False)(new_md)
        logs_data_interpolated.update({(log.dataset_id, log._id): interpolated_values})

    # splice log_names
    if not logs_data_interpolated:
        raise AttributeError(f"Cannot get any data for splicing. Check log units first.")
    df = pd.DataFrame(logs_data_interpolated)

    logs_order = []
    for log in sorted(logs, key=lambda x: x.meta['run']['value'], reverse=True):
        if (log.dataset_id, log._id) in logs_data_interpolated:
            logs_order.append((log.dataset_id, log._id))

    result = df[logs_order].bfill(axis=1).iloc[:, 0]

    return np.vstack((new_md, result)).T


class SpliceLogsNode(EngineNode):
    """
    Runs log splicing in all wells in the project
    """

    @staticmethod
    def calculate_for_well(wellname: str, datasetnames: list[str] = None, logs: list[str] = None, tags: list[str] = None, output_dataset_name: str = DEFAULT_LQC_NAME):
        """
            Method to splice logs in a well. Takes logs in datasets and outputs it into a specified output dataset
            :param wellname: Well name as string
            :param datasetnames: Datasets' name as list of strings. If None then uses all datasets
            :param logs: Logs' names as list of string. If None then uses all logs available in datasets
            :param tags: tags that must be in logs to process the logs (one of)
            :param output_dataset_name: Name of output dataset
            """
        w = Well(wellname)
        logs_data, logs_meta = splice_logs(w, datasetnames, logs, tags)
        wd = WellDataset(w, output_dataset_name, new=True)
        for log_name in logs_data.keys():
            log = BasicLog(wd.id, log_name)
            log.values = logs_data[log_name]
            log.meta = logs_meta[log_name]
            log.meta.add_tags('spliced')
            log.save()

    @classmethod
    def run(cls, output_dataset_name: str = "LQC", async_job: bool = True):
        """
        Run log splicing calculations
        :param output_dataset_name:
        :param async_job: run via Celery or in this process
        :return:
        """
        p = Project()
        if async_job:
            tasks = [
                celery_app.send_task('tasks.async_splice_logs', kwargs={
                    'wellname': well_name,
                    'tags': ['processing', ],
                    'output_dataset_name': output_dataset_name}
                                     )
                for well_name in p.list_wells()
            ]
            wait_till_completes(tasks)
        else:
            for well_name in p.list_wells():
                cls.calculate_for_well(**{
                    'wellname': well_name,
                    'tags': ['processing', ],
                    'output_dataset_name': output_dataset_name}
                                       )


if __name__ == '__main__':
    node = SpliceLogsNode()
    node.run(async_job=False)
