import logging
import time

import numpy  as np

import celery_conf
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode


class PorosityFromDensityNode(EngineNode):
    """
    Porosity from Density calculations
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    @staticmethod
    def linear_method(log, rhob_matrix: float, rhob_fluid: float, output_log_name: str) -> BasicLog:
        """
        Function to calculate Porosity from Bulk Density log (PHIT_D) via linear method
        :param log: BasicLog
        :param rhob_matrix: float, RHOB value at matrix
        :param rhob_fluid: float, RHOB value at fluid
        :return: BasicLog (virtual)
        """
        output_log_id = "PHIT_D"

        phit_d = BasicLog(log_id=output_log_id)

        phit_d.meta.family = "Total Porosity"
        phit_d.meta.method = "Total Porosity derived from Bulk Density log via linear method"

        values = log.values
        values[:, 1] = np.clip((rhob_matrix - values[:, 1]) / (rhob_matrix - rhob_fluid), 0, 1)

        phit_d.values = values
        # phit_d.meta.basic_statistics = get_basic_curve_statistics(phit_d.values)
        phit_d.meta.name = output_log_name
        phit_d.meta.log_id = output_log_id
        phit_d.meta.family = 'Porosity'
        phit_d.meta.method = 'Linear method from density'
        phit_d.meta.units = '%'

        return phit_d

    @classmethod
    def run_for_item(cls, **kwargs) -> None:
        """
        Function to calculate Porosity from Bulk Density log (PHIT_D) via linear method
        :param well_name: name of well to process
        :param rhob_matrix: float, RHOB value at matrix
        :param rhob_fluid: float, RHOB value at fluid
        :param output_log_name: default, PHIT_D
        """
        well_name = kwargs['well_name']
        rhob_matrix = kwargs['rhob_matrix']
        rhob_fluid = kwargs['rhob_fluid']
        output_log_name = kwargs.get('output_log_name', 'PHIT_D')

        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')

        for log_name in dataset.log_list:
            log = BasicLog(dataset.id, log_name)

            if 'family' not in log.meta or log.meta.family not in ['Density', 'Bulk Density']:
                continue

            if rhob_matrix is None:
                rhob_matrix = 2.65  # g/cm3
            if rhob_fluid is None:
                rhob_fluid = 1.05  # g/cm3

            output = cls.linear_method(log, rhob_matrix, rhob_fluid, output_log_name)
            output.dataset_id = dataset.id
            cls.write_history(log=output,
                              input_logs=(log,),
                              parameters=kwargs)
            output.save()

    @classmethod
    def run(cls, **kwargs):

        rhob_matrix = kwargs.get('rhob_matrix', None)
        rhob_fluid = kwargs.get('rhob_matrix', None)
        output_log_name = kwargs.get('output_log_name', 'PHIT_D')

        p = Project()
        well_names = p.list_wells()
        tasks = []
        for well_name in well_names:
            celery_conf.app.send_task('tasks.async_calculate_porosity_from_density', (well_name, rhob_matrix, rhob_fluid, output_log_name))

        cls.track_progress(tasks)

    @classmethod
    def write_history(cls, **kwargs):
        log = kwargs['log']
        input_logs = kwargs['input_logs']
        parameters = kwargs['parameters']

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': parameters
                                 })
