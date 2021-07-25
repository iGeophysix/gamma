import logging
import time
from abc import ABC
from typing import Any

import celery_conf
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

    @classmethod
    def track_progress(cls, engine_progress, tasks):
        """
        Tracks progress in EngineProgress object (observer)
        :param engine_progress: observer
        :param tasks: Celery tasks list
        :return:
        """
        while True:
            progress = celery_conf.track_progress(tasks)
            engine_progress.update(cls.name(), progress)
            if progress['completion'] == 1:
                break
            time.sleep(0.1)
