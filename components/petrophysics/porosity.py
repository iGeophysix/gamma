import numpy  as np

from components.domain.Log import BasicLog
from components.petrophysics.curve_operations import get_basic_curve_statistics


def linear_method(log, rhob_matrix: float, rhob_fluid: float) -> BasicLog:
    """
    Function to calculate Porosity from Bulk Density log (PHIT_D) via linear method
    :param log: BasicLog
    :param rhob_matrix: float, RHOB value at matrix
    :param rhob_fluid: float, RHOB value at fluid
    :return: BasicLog (virtual)
    """
    phit_d = BasicLog(id='PHIT_D')

    phit_d.meta = log.meta
    phit_d.log_family = "Total Porosity"
    phit_d.meta = phit_d.meta | {"method": "Total Porosity derived from Bulk Density log via linear method"}

    values = log.values
    values[:, 1] = np.clip((rhob_matrix - values[:, 1]) / (rhob_matrix - rhob_fluid), 0, 1)

    phit_d.values = values
    basic_stats = get_basic_curve_statistics(phit_d.values)
    phit_d.meta = phit_d.meta | {'basic_statistics': basic_stats}

    return phit_d
