from abc import ABC, abstractmethod
from typing import Any

import logging

from settings import LOGGING_LEVEL


class EngineNode(ABC):
    """
    This class describes general object for all computational nodes
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(LOGGING_LEVEL)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    # @abstractmethod
    def version(cls):
        pass

    @classmethod
    # @abstractmethod
    def run_for_item(cls, **kwargs):
        pass

    @classmethod
    # @abstractmethod
    def run(cls, **kwargs) -> Any:
        """
        Run calculations
        :param args: positional arguments
        :param kwargs: keyword arguments
        """
        pass

    @classmethod
    # @abstractmethod
    def write_history(cls, **kwargs):
        """
        self.history = [
                        {'node': <NodeClassName>,
                         'node_version': 4,
                         'event_timestamp':'2021-05-21 14:32:54.3123',
                         'parent_logs': ((<dataset_id>, <log_id>),...),
                         'parameters':{....}
                         },...
                       ]
        """
        pass
