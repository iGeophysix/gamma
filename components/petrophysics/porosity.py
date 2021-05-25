import logging

import numpy  as np

import celery_conf
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode
from components.petrophysics.curve_operations import get_basic_curve_statistics


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
    def calculate_for_well(cls, well_name: str, rhob_matrix: float = None, rhob_fluid: float = None, output_log_name: str = 'PHIT_D') -> None:
        """
        Function to calculate Porosity from Bulk Density log (PHIT_D) via linear method
        :param well_name: name of well to process
        :param rhob_matrix: float, RHOB value at matrix
        :param rhob_fluid: float, RHOB value at fluid
        :param output_log_name: default, PHIT_D
        """
        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')

        for log_name in dataset.log_list:
            log = BasicLog(dataset.id, log_name)

            if not hasattr(log.meta, 'family') or log.meta.family not in ['Density', 'Bulk Density']:
                continue

            if rhob_matrix is None:
                rhob_matrix = 2.65  # g/cm3
            if rhob_fluid is None:
                rhob_fluid = 1.05  # g/cm3

            output = cls.linear_method(log, rhob_matrix, rhob_fluid, output_log_name)
            output.dataset_id = dataset.id
            output.save()

    @classmethod
    def run(cls, rhob_matrix: float = None, rhob_fluid: float = None, output_log_name: str = 'PHIT_D'):
        p = Project()
        well_names = p.list_wells()
        tasks = []
        for well_name in well_names:
            celery_conf.app.send_task('tasks.async_calculate_porosity_from_density', (well_name, rhob_matrix, rhob_fluid, output_log_name))

        celery_conf.wait_till_completes(tasks)