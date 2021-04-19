import numpy as np

from components.domain.Log import BasicLog
from components.petrophysics.curve_operations import get_basic_curve_statistics
from components.petrophysics.engine_module import EngineModule


def _linear_scale(arr, lower_limit, upper_limit):
    inv_range = 1 / (upper_limit - lower_limit)
    offset = lower_limit * inv_range

    result = arr * inv_range - offset
    return result


class ShaleVolumeLinearMethod(EngineModule):
    """
    Shale volume calculations
    """

    class Meta:
        name = 'Shale Volume Linear Method'
        input = {
            "type": BasicLog,
            "meta": {
                "log_family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "id": "VSH_GR_LM",
            "meta": {
                "log_family": "Shale Volume",
                "method": "Linear method based on Gamma Ray logs",
            }
        }

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
    def run(cls, log, gr_matrix: float, gr_shale: float, name: str = None) -> BasicLog:
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

        vsh = cls_output['type'](id=cls_output['id'])
        vsh.name = cls_output['id'] if name is None else name

        values = log.values
        values[:, 1] = np.clip(_linear_scale(values[:, 1], gr_matrix, gr_shale), 0, 1)
        vsh.values = values
        basic_stats = get_basic_curve_statistics(vsh.values)
        vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

        return vsh


class ShaleVolumeLarionovOlderRock(EngineModule):
    """
    Shale volume calculations using Larionov Older Rock
    """

    class Meta:
        name = 'Shale Volume Larionov Older Rock'
        input = {
            "type": BasicLog,
            "meta": {
                "log_family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "id": "VSH_GR_LOR",
            "meta": {
                "log_family": "Shale Volume",
                "method": "Larionov older rock method based on Gamma Ray logs",
            }
        }

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
    def run(cls, log, gr_matrix: float, gr_shale: float, name: str = None) -> BasicLog:
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

        vsh = cls_output['type'](id=cls_output['id'])
        vsh.name = cls_output['id'] if name is None else name
        vsh.meta = log.meta
        vsh.log_family = cls_output['meta']['log_family']
        vsh.meta = vsh.meta | {"method": cls_output['meta']['method']}

        values = log.values
        gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
        values[:, 1] = np.clip(0.33 * (2 ** (2 * gr_index) - 1), 0, 1)
        vsh.values = values
        basic_stats = get_basic_curve_statistics(vsh.values)
        vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

        return vsh


class ShaleVolumeLarionovTertiaryRock(EngineModule):
    """
    Shale volume calculations using Larionov Tertiary Rock
    """

    class Meta:
        name = 'Shale Volume Larionov Tertiary Rock'
        input = {
            "type": BasicLog,
            "meta": {
                "log_family": "Gamma Ray",
            },
        }
        output = {
            "type": BasicLog,
            "id": "VSH_GR_LTR",
            "meta": {
                "log_family": "Shale Volume",
                "method": "Linear method based on Gamma Ray logs",
            }
        }

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
    def run(cls, log, gr_matrix: float, gr_shale: float, name: str = None) -> BasicLog:
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

        vsh = cls_output['type'](id=cls_output['id'])
        vsh.name = cls_output['id'] if name is None else name
        vsh.meta = log.meta
        vsh.log_family = cls_output['meta']['log_family']
        vsh.meta = vsh.meta | {"method": cls_output['meta']['method']}

        values = log.values
        gr_index = _linear_scale(values[:, 1], gr_matrix, gr_shale)
        values[:, 1] = np.clip(0.083 * (2 ** (3.7 * gr_index) - 1), 0, 1)
        vsh.values = values
        basic_stats = get_basic_curve_statistics(vsh.values)
        vsh.meta = vsh.meta | {'basic_statistics': basic_stats}

        return vsh
