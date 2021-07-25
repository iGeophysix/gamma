import logging
import time

import numpy as np
import pandas as pd
import scipy.interpolate

import celery_conf
from celery_conf import app as celery_app
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.UnitsSystem import UnitConversionError
from settings import DEFAULT_LQC_NAME

logger = logging.getLogger('LogSplicingNode')


def splice_logs(well: Well,
                dataset_names: list[str] = None,
                log_names: list[str] = None,
                tags: list[str] = None) -> tuple[dict[str, np.ndarray], dict[str, dict]]:
    """
    This function processes the well and generates a dataset with spliced logs and its meta information
    :param well: Well object to process
    :param dataset_names: Optional. List of str with dataset names. If None then uses all datasets
    :param log_names: Optional. List of str with logs names. Logs must present in all datasets. If None then uses all logs in all datasets
    :param tags: tags that must be in logs to process the logs (one of)
    :return: dict with log data and log meta
    """
    FAMILY_PROPERTIES = FamilyProperties()

    logs = []
    families_to_splice = {'Formation Resistivity'}  # persistent family for special splicing procedure
    for dataset_name in (dataset_names if dataset_names is not None else well.datasets):
        wd = WellDataset(well, dataset_name)
        for log_name in (log_names if log_names is not None else wd.log_list):
            log = BasicLog(wd.id, log_name)
            log_tags = set(log.meta.tags)
            if tags is None or log_tags.intersection(tags):  # any of tags must be present
                splice_family = FAMILY_PROPERTIES[log.meta.family].get('splice', False)
                if splice_family:
                    families_to_splice.add(log.meta.family)
                if splice_family or 'best_rt' in log_tags:  # add any family best_rt logs also
                    logs.append(log)

    # here and after each log family must be spliced by families and then runs
    results_data = {}
    results_meta = {}

    for family in families_to_splice:
        family_meta = FAMILY_PROPERTIES[family]

        # input logs filter
        if family == 'Formation Resistivity':
            suitable_log = lambda log: 'best_rt' in log.meta.tags
        else:
            suitable_log = lambda log: log.meta.family == family

        logs_to_splice = [log for log in logs if suitable_log(log)]
        if not logs_to_splice:
            continue

        # splice log_names
        target_units = family_meta['unit']
        try:
            spliced = splice_logs_in_family(logs_to_splice, target_units=target_units)
        except AttributeError as exc:
            logger.warning(str(exc) + f" Well {well.name}. Family: {family}")
            continue

        # define meta information
        meta = {}
        meta['AutoSpliced'] = {'Intervals': len(logs_to_splice), 'Uncertainty': 0.5}
        meta['family'] = family
        meta['units'] = target_units
        results_data.update({family_meta['mnemonic']: spliced})  # Log name will be defined here
        results_meta.update({family_meta['mnemonic']: meta})  # log name will be defined here

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
        else:
            log_values = log.values
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
    def calculate_for_well(wellname: str,
                           datasetnames: list[str] = None,
                           logs: list[str] = None,
                           tags: list[str] = None,
                           output_dataset_name: str = DEFAULT_LQC_NAME):
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
    def run(cls, **kwargs):
        """
        Run log splicing calculations
        :param output_dataset_name:
        :param async_job: run via Celery or in this process
        :return:
        """
        output_dataset_name = kwargs.get('output_dataset_name', "LQC")
        async_job = kwargs.get('async_job', True)
        p = Project()
        if async_job:
            tasks = []

            for well_name in p.list_wells():
                params = {'wellname': well_name,
                          'tags': ['processing', ],
                          'output_dataset_name': output_dataset_name
                          }
                tasks.append(celery_app.send_task('tasks.async_splice_logs', kwargs=params))

            engine_progress = kwargs['engine_progress']
            while True:
                progress = celery_conf.track_progress(tasks)
                engine_progress.update(cls.name(), progress)
                if progress['completion'] == 1:
                    break
                time.sleep(0.1)

        else:
            for well_name in p.list_wells():
                params = {'wellname': well_name,
                          'tags': ['processing', ],
                          'output_dataset_name': output_dataset_name
                          }
                cls.calculate_for_well(**params)


if __name__ == '__main__':
    node = SpliceLogsNode()
    node.run(async_job=False)
