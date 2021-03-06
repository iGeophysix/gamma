import hashlib
import json
import logging
import time
from abc import ABC
from typing import Any

import celery_conf
from components.database.RedisStorage import RedisStorage
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
    def version(cls):
        pass

    @classmethod
    def run_for_item(cls, **kwargs):
        pass

    @staticmethod
    def item_md5(item) -> str:
        """Get item md5 as string"""
        return hashlib.md5(json.dumps(item).encode()).hexdigest()

    @classmethod
    def item_hash(cls, *args) -> tuple[str, bool]:
        """Get item hash to use in cache
        :return item_value: hash value of the item
        :return valid: if hash is still valid for the object
        """
        item_value = ''
        valid = False
        return item_value, valid

    @classmethod
    def run(cls, **kwargs) -> Any:
        """
        Run calculations
        :param args: positional arguments
        :param kwargs: keyword arguments
        """
        pass

    @classmethod
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
    def track_progress(cls, tasks, cached=0):
        """
        Tracks progress in EngineProgress object (observer)
        :param tasks: Celery tasks list
        :param cached: number of cached tasks
        :return:
        """
        engine_progress = EngineProgress()
        while True:
            progress = celery_conf.track_progress(tasks, cached)
            engine_progress.update(cls.name(), progress)
            if progress['completion'] == 1:
                break
            time.sleep(0.1)


class EngineNodeCache:
    """Engine Node Cache to skip already calculated items"""

    storage_table_name = 'engine_node_cache'

    def __init__(self, node):
        """
        Initialize EngineNodeCache object
        :param name: node name
        :param version: version name
        """
        self._name = node.name()
        self._version = node.version()
        self._cache = self._load()

    def _load(self):
        """Loads cache from DB"""
        s = RedisStorage()
        if s.table_key_exists(self.storage_table_name, self._name):
            cache = s.table_key_get(self.storage_table_name, self._name)
            # check that node cache version equals to current node version, else return empty cache
            if cache['version'] == self._version:
                return cache

        return {
            'node': self._name,
            'version': self._version,
            'items': [],
        }

    def set(self, items):
        """Set items in cache"""
        self._cache['items'] = items
        self.save()

    def clear(self):
        """Clear cache items"""
        self._cache['items'] = []

    def save(self):
        """Save cache to storage"""
        s = RedisStorage()
        s.table_key_set(self.storage_table_name, self._name, self._cache)

    def __contains__(self, item):
        return item in self._cache['items']

    def __len__(self):
        return len(self._cache['items'])

    @property
    def node(self):
        """Get Node name"""
        return self._cache['node']

    @property
    def version(self):
        """Get Node version"""
        return self._cache['version']


class EngineProgress:
    def __init__(self):
        self._s = RedisStorage()
        if self._s.object_exists('engine_runs'):
            self._nodes = json.loads(self._s.object_get('engine_runs'))
        else:
            self._nodes = {}

    def save(self):
        '''Save updates to observer'''
        self._s.object_set('engine_runs', json.dumps(self._nodes).encode())

    def update(self, node_name, node_status):
        '''
        Update nodes status
        :param node_name:
        :param node_status:
        :return:
        '''
        self._nodes[node_name] = node_status
        self.save()

    def clear(self):
        """Clear results"""
        self._nodes = {}
        self.save()
