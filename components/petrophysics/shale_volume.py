import logging

import numpy as np

import celery_conf
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache

logging.basicConfig()


def _linear_scale(arr, lower_limit, upper_limit):
    inv_range = 1 / (upper_limit - lower_limit)
    offset = lower_limit * inv_range

    result = arr * inv_range - offset
    return result


class ShaleVolume(EngineNode):
    """
    Shale volume calculations
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 1

    @classmethod
    def validate_input(cls, log: BasicLog, gr_matrix: float, gr_shale: float, name: str) -> None:
        """
        Validate inputs
        :param log:
        :param gr_matrix:
        :param gr_shale:
        :param name:
        """
        # check types
        if not isinstance(log, BasicLog):
            raise TypeError("Log argument is not instance of class BasicLog")
        if not isinstance(gr_matrix, (float, int)):
            raise TypeError("gr_matrix is not of type float")
        if not isinstance(gr_shale, (float, int)):
            raise TypeError("gr_shale is not of type float")

        # check gr_matrix is greater than or equals gr_shale
        if gr_matrix >= gr_shale:
            raise ValueError("gr_matrix must be lower than gr_shale")

        # check log input
        valid_meta_parameters = cls.Meta.input['meta']
        for parameter, value in valid_meta_parameters.items():
            assert log.meta[parameter] == value, f"Meta parameter {parameter} must be equal {value}."

        # check name is valid
        if name is not None:
            assert type(name) == str, f"name must be of type string"

    @classmethod
    def _calculate(cls, log: BasicLog, gr_matrix: float, gr_shale: float, name: str) -> BasicLog:
        raise Exception("You are using abstract class instead of")

    @classmethod
    def run_for_item(cls, well_name: str, gr_matrix: float = None, gr_shale: float = None, output_log_name: str = 'VSH_GR') -> None:
        """
        Function to calculate Shale Volume (VSH) via Linear method
        :param well_name: well name to process
        :param gr_matrix: float, Gamma Ray value at matrix
        :param gr_shale: float, Gamma Ray value at shale
        :param output_log_name: name of output log
        """

        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')

        for log_name in dataset.get_log_list(family='Gamma Ray'):
            log = BasicLog(dataset.id, log_name)

            if 'spliced' in log.meta.tags:
                continue

            if gr_matrix is None:
                gr_matrix = np.quantile(log.non_null_values[:, 1], 0.05)
            if gr_shale is None:
                gr_shale = np.quantile(log.non_null_values[:, 1], 0.95)

            output = cls._calculate(log, gr_matrix, gr_shale, output_log_name)
            output.dataset_id = dataset.id
            cls.write_history(log=output, input_logs=(log,), gr_matrix=gr_matrix, gr_shale=gr_shale)
            output.save()

    @classmethod
    def item_hash(cls, well_name, gr_matrix, gr_shale, output_log_name) -> tuple[str, bool]:
        """Get current item hash"""
        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')
        log_hashes = []
        for log_name in dataset.get_log_list(family='Gamma Ray'):
            log = BasicLog(dataset.id, log_name)
            if 'spliced' in log.meta.tags:
                log_hashes.append(log.data_hash)

        item_hash = cls.item_md5((well_name, sorted(log_hashes), gr_matrix, gr_shale, output_log_name))

        valid = BasicLog(dataset.id, output_log_name).exists()

        return item_hash, valid

    @classmethod
    def run(cls, **kwargs):
        """Run shale volume calculation for well"""
        gr_matrix = kwargs.get('gr_matrix', None)
        gr_shale = kwargs.get('gr_shale', None)
        output_log_name = kwargs.get('output_log_name', 'VSH_GR')

        p = Project()
        well_names = p.list_wells()
        tasks = []

        hashes = []
        cache_hits = 0
        cache = EngineNodeCache(cls)

        for well_name in well_names:

            item_hash, item_hash_is_valid = cls.item_hash(well_name, gr_matrix, gr_shale, output_log_name)
            hashes.append(item_hash)
            if item_hash_is_valid and item_hash in cache:
                cache_hits += 1
                continue

            tasks.append(celery_conf.app.send_task('tasks.async_calculate_shale_volume', (well_name, cls.__name__, gr_matrix, gr_shale, output_log_name)))

        cache.set(hashes)
        cls.logger.info(f'Node: {cls.name()}: cache hits:{cache_hits} / misses: {len(tasks)}')
        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def write_history(cls, **kwargs):
        log = kwargs['log']
        input_logs = kwargs['input_logs']
        gr_matrix = kwargs.get('gr_matrix', None)
        gr_shale = kwargs.get('gr_shale', None)

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': {'gr_matrix': gr_matrix, 'gr_shale': gr_shale, }
                                 })


class ShaleVolumeLinearMethodNode(ShaleVolume):
    """
    Shale volume calculations
    """

    logger = logging.getLogger("ShaleVolumeLinearMethodNode")
    logger.setLevel(logging.INFO)

    class Meta:
        name = 'Shale Volume Linear Method'
        input = {
            "type": BasicLog,
            "meta": {
                "family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "log_id": "VSH_GR_LM",
            "meta": {
                "family": "Shale Volume",
                "method": "Linear method based on Gamma Ray logs",
            }
        }

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 1

    @classmethod
    def _calculate(cls, log: BasicLog, gr_matrix: float, gr_shale: float, name: str) -> BasicLog:
        cls_output = cls.Meta.output

        vsh = cls_output['type'](log_id=cls_output['log_id'])

        values = log.values
        values[:, 1] = np.clip(_linear_scale(values[:, 1], gr_matrix, gr_shale), 0, 1)
        vsh.values = values

        vsh.meta.name = cls_output['log_id'] if name is None else name
        vsh.meta.log_id = cls_output['log_id']
        vsh.meta.family = cls_output['meta']['family']
        vsh.meta.method = cls_output['meta']['method']
        vsh.meta.units = 'v\\v'

        return vsh


class ShaleVolumeLarionovOlderRockNode(ShaleVolume):
    """
    Shale volume calculations using Larionov Older Rock
    """

    class Meta:
        name = 'Shale Volume Larionov Older Rock'
        input = {
            "type": BasicLog,
            "meta": {
                "family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "log_id": "VSH_GR_LOR",
            "meta": {
                "family": "Shale Volume",
                "method": "Larionov older rock method based on Gamma Ray logs",
            }
        }

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 1

    @classmethod
    def _calculate(cls, log, gr_matrix: float, gr_shale: float, name: str = None) -> BasicLog:
        """
        Function to calculate Shale Volume (VSH) via Larionov older rock methods
        :param log: BasicLog
        :param gr_matrix: float, Gamma Ray value at matrix
        :param gr_shale: float, Gamma Ray value at shale
        :param name: str, Name of output log
        :return: BasicLog (virtual)
        """
        cls.validate_input(log, gr_matrix, gr_shale, name)
        cls_output = cls.Meta.output

        vsh = cls_output['type'](log_id=cls_output['log_id'])

        values = log.values
        gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
        values[:, 1] = np.clip(0.33 * (2 ** (2 * gr_index) - 1), 0, 1)
        vsh.values = values

        vsh.meta.name = cls_output['log_id'] if name is None else name
        vsh.meta.log_id = cls_output['log_id']
        vsh.meta.family = cls_output['meta']['family']
        vsh.meta.method = cls_output['meta']['method']
        vsh.meta.units = 'v\\v'
        return vsh


class ShaleVolumeLarionovTertiaryRockNode(ShaleVolume):
    """
    Shale volume calculations using Larionov Tertiary Rock
    """

    class Meta:
        name = 'Shale Volume Larionov Tertiary Rock'
        input = {
            "type": BasicLog,
            "meta": {
                "family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "log_id": "VSH_GR_LTR",
            "meta": {
                "family": "Shale Volume",
                "method": "Larionov Tertiary Rock based on Gamma Ray logs",
            }
        }

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def _calculate(cls, log, gr_matrix: float, gr_shale: float, name: str = None) -> BasicLog:
        """
        Function to calculate Shale Volume (VSH) via Larionov tertiary rock methods
        :param log: BasicLog
        :param gr_matrix: float, Gamma Ray value at matrix
        :param gr_shale: float, Gamma Ray value at shale
        :param name: str, name of output log
        :return: BasicLog (virtual)
        """
        cls.validate_input(log, gr_matrix, gr_shale, name)
        cls_output = cls.Meta.output

        vsh = cls_output['type'](log_id=cls_output['log_id'])

        values = log.values
        gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
        values[:, 1] = np.clip(0.083 * (2 ** (3.7 * gr_index) - 1), 0, 1)
        vsh.values = values

        vsh.meta.name = cls_output['log_id'] if name is None else name
        vsh.meta.log_id = cls_output['log_id']
        vsh.meta.family = cls_output['meta']['family']
        vsh.meta.method = cls_output['meta']['method']
        vsh.meta.units = 'v\\v'
        return vsh
