from abc import ABC
from typing import Any


class EngineNode(ABC):
    """
    This class describes general object for all computational nodes
    """

    class Meta:
        name = None
        input = None
        output = None

    @classmethod
    def validate_input(cls, *args, **kwargs):
        """
        Check if input is valid for this. If all is OK - return None, else raise an exception
        :param args: positional arguments to validate
        :param kwargs: keyword arguments to validate
        """
        pass

    @classmethod
    def run(cls, *args, **kwargs) -> Any:
        """
        Run calculations
        :param args: positional arguments
        :param kwargs: keyword arguments
        """
        cls.validate_input(*args, **kwargs)
