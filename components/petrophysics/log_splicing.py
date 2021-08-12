import logging
from datetime import datetime

import numpy as np
import pandas as pd
import scipy.interpolate

from celery_conf import app as celery_app
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.UnitsSystem import UnitConversionError
from settings import DEFAULT_LQC_NAME, LOGGING_LEVEL

logger = logging.getLogger('LogSplicingNode')


class SpliceLogsNode(EngineNode):
    """
    Runs log splicing in all wells in the project
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(LOGGING_LEVEL)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        pass

    @classmethod
    def splice_logs(cls,
                    well: Well,
                    dataset_names: list[str] = None,
                    log_names: list[str] = None,
                    tags: list[str] = None,
                    output_dataset_name: str = 'LQC') -> None:
        """
        This function processes the well and generates a dataset with spliced logs and its meta information
        :param well: Well object to process
        :param dataset_names: Optional. List of str with dataset names. If None then uses all datasets
        :param log_names: Optional. List of str with logs names. Logs must present in all datasets. If None then uses all logs in all datasets
        :param tags: tags that must be in logs to process the logs (one of)
        :param output_dataset_name: name of output dataset. Default: 'LQC'
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

        results_ds = WellDataset(well, output_dataset_name, new=True)
        for family in families_to_splice:
            family_meta = FAMILY_PROPERTIES[family]

            # input logs filter
            if family == 'Formation Resistivity':
                logs_to_splice = [log for log in logs if 'best_rt' in log.meta.tags]
            else:
                logs_to_splice = [log for log in logs if log.meta.family == family]

            if not logs_to_splice:
                continue

            # splice log_names
            target_units = family_meta['unit']
            try:
                spliced = cls.splice_logs_in_family(logs_to_splice, target_units=target_units)
            except AttributeError as exc:
                logger.warning(str(exc) + f" Well {well.name}. Family: {family}")
                continue

            # define meta information

            log = BasicLog(results_ds.id, family_meta['mnemonic'])
            log.values = spliced
            log.meta.AutoSpliced = {'Intervals': len(logs_to_splice), 'Uncertainty': 0.5}
            log.meta.family = family
            log.meta.units = target_units
            log.meta.add_tags('spliced')
            cls.write_history(log=log, input_logs=logs_to_splice, parameters={})
            log.save()

    @staticmethod
    def splice_logs_in_family(logs: list, target_units: str = None) -> np.ndarray:
        '''
        This function takes all log_names defined in log_names meta and splices them into one
        Docs: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/f/Logs-Splicing-45J9iu3UhOtI
        :param logs: list of BasicLog
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

    @classmethod
    def run_for_item(cls, **kwargs):
        """
        Method to splice logs in a well. Takes logs in datasets and outputs it into a specified output dataset
        :param wellname: Well name as string
        :param datasetnames: Datasets' name as list of strings. If None then uses all datasets
        :param logs: Logs' names as list of string. If None then uses all logs available in datasets
        :param tags: tags that must be in logs to process the logs (one of)
        :param output_dataset_name: Name of output dataset
        """
        wellname = kwargs.get('wellname')
        datasetnames = kwargs.get('datasetnames', None)
        logs = kwargs.get('logs', None)
        tags = kwargs.get('tags', None)
        output_dataset_name = kwargs.get('output_dataset_name', DEFAULT_LQC_NAME)

        w = Well(wellname)
        cls.splice_logs(w, datasetnames, logs, tags, output_dataset_name)


    @classmethod
    def item_hash(cls, *args) -> tuple[str, bool]:
        """Get item hash to use in cache
        :return item_value: hash value of the item
        :return valid: if hash is still valid for the object
        """
        wellname = args[0]['wellname']
        tags = args[0]['tags']
        output_dataset_name = args[0]['output_dataset_name']

        log_hashes = []
        well = Well(wellname)
        log_families = set()
        for ds_name in well.datasets:
            if ds_name == 'LQC':
                continue
            ds = WellDataset(well, ds_name)
            for log_id in ds.log_list:
                log = BasicLog(ds.id, log_id)
                if any(tag in log.meta.tags for tag in tags) and FamilyProperties()[log.meta.family]['splice']:
                    log_hashes.append(log.meta.data_hash)
                    log_families.add(log.meta.family)
        item_hash = cls.item_md5((wellname, tuple(sorted(log_hashes)), output_dataset_name))

        lqc_ds = WellDataset(well, output_dataset_name)
        valid_logs = []
        for log_id in lqc_ds.get_log_list(family__in=log_families):
            log = BasicLog(lqc_ds.id, log_id)
            if 'spliced' in log.meta.tags:
                valid_logs.append(log)

        valid = len(valid_logs) == len(list(log_families))

        return item_hash, valid

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

        hashes = []
        cache_hits = 0
        cache = EngineNodeCache(cls)

        p = Project()
        if async_job:
            tasks = []

            for well_name in p.list_wells():
                params = {'wellname': well_name,
                          'tags': ['processing', ],
                          'output_dataset_name': output_dataset_name
                          }
                item_hash, item_hash_is_valid = cls.item_hash(params)
                hashes.append(item_hash)
                if item_hash_is_valid and item_hash in cache:
                    cache_hits += 1
                    continue

                tasks.append(celery_app.send_task('tasks.async_splice_logs', kwargs=params))

            cache.set(hashes)
            cls.logger.info(f'Node: {cls.name()}: cache hits:{cache_hits} / misses: {len(tasks)}')
            cls.track_progress(tasks, cached=cache_hits)

        else:
            for well_name in p.list_wells():
                params = {'wellname': well_name,
                          'tags': ['processing', ],
                          'output_dataset_name': output_dataset_name
                          }
                cls.run_for_item(**params)

    @classmethod
    def write_history(cls, **kwargs):
        log = kwargs['log']
        input_logs = kwargs['input_logs']
        parameters = kwargs['parameters']

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'timestamp': datetime.now().isoformat(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': parameters
                                 })


if __name__ == '__main__':
    node = SpliceLogsNode()
    node.run(async_job=True)
