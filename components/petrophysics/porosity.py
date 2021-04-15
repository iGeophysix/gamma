import numpy  as np

from components.domain.Log import BasicLog


def linear_method(log, rhob_matrix: float, rhob_fluid: float) -> BasicLog:
    """
    Function to calculate Porosity from Bulk Density log (PHIT_D) via linear method
    :param log: BasicLog
    :param rhob_matrix: float, RHOB value at matrix
    :param rhob_fluid: float, RHOB value at fluid
    :return: BasicLog (virtual)
    """
    vsh = BasicLog(id='PHIT_D')

    vsh.meta = log.meta
    vsh.log_family = "Total Porosity"
    vsh.meta = vsh.meta | {"method": "Total Porosity derived from Bulk Density log via linear method"}

    values = log.values
    scale = 1 / (rhob_matrix - rhob_fluid)
    offset = rhob_matrix * scale
    values[:, 1] = np.clip(offset - values[:, 1] / scale, 0, 1)
    vsh.values = values

    return vsh
