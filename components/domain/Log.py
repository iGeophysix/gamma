import copy
from datetime import datetime

import numpy as np

from components.database.RedisStorage import RedisStorage as Storage


class BasicLog:
    """
    General class for all types of logs
    """

    def __init__(self, dataset_id: str = None, id: str = 'New Log'):
        """
        Init method
        :param dataset_id: Parent dataset_id
        :param name: Log name
        """
        self._dataset_id = dataset_id
        self._id = id
        self._type = 'BasicLog'
        self._values = None
        self._meta = None
        self.depth = None
        self.depth__gt = None
        self.depth__lt = None
        self._changes = {"values": False, "meta": False}

    def __str__(self) -> str:
        """
        Formatter of the log when it is called in print
        :return:
        """
        return self.name

    def __len__(self) -> int:
        """
        :return: Number of samples
        """
        return self.values.__len__()

    def __getitem__(self, key, ):
        """
        Proxy to data when using with square brackets
        :param key: use as with slicing np.array
        :return: np.array with sliced data
        """
        return self.values.__getitem__(key, )

    def exists(self) -> bool:
        """
        Check if the log is exists in the dataset
        :return: bool
        """
        _s = Storage()
        return _s.check_log_exists(self._dataset_id, self._id)

    @property
    def name(self) -> str:
        """
        Returns log name from meta
        :return:
        """
        if not "name" in self.meta:
            self.name = self._id
        return self.meta['name']

    @name.setter
    def name(self, name: str) -> None:
        """
        Set log name in meta
        :return:
        """

        self.meta = self.meta | {"name": name}

    @property
    def dataset_id(self) -> str:
        """
        Get current dataset id
        :return: dataset id as string
        """
        return self._dataset_id

    @dataset_id.setter
    def dataset_id(self, dataset_id) -> None:
        """Assign dataset id to the log"""
        self._dataset_id = dataset_id

    @property
    def values(self) -> np.array:
        """
        Get log values
        :return: np.array with log values
        """
        if self._values is None:
            self._fetch()
        return self._values

    @values.setter
    def values(self, values: np.array) -> None:
        """
        Set log values
        :param values: np.array with data
        :return: None
        """
        if not self.validate(values):
            raise ValueError("Data is not passing validation")
        self._values = values
        self._changes['values'] = True
        if self._values is None:
            self._fetch()

    @property
    def non_null_values(self) -> np.array:
        """
        Get all non-empty log values
        :return: np.array with log values
        """
        if self._values is None:
            self._fetch()
        return self._values[~np.isnan(self._values[:, 1])]

    @property
    def meta(self) -> dict:
        """
        Get log meta information
        :return: dictionary with log meta information
        """
        if self.exists() and self._meta is None:
            _s = Storage()
            self._meta = _s.get_log_meta(self._dataset_id, self._id)
        elif self._meta is None:
            self._meta = {
                'name': self._id
            }
        return self._meta

    @meta.setter
    def meta(self, meta: dict) -> None:
        """
        Set log meta information
        :return: None
        """
        self._meta = meta | {'__type': self._type}
        self._changes['meta'] = True

    @property
    def history(self) -> list[tuple]:
        """
        Get whole history of the log curve
        :return:
        """
        _s = Storage()
        return _s.log_history(self._dataset_id, self._id)

    @history.setter
    def history(self, text) -> None:
        """
        Appends an event to history
        :param text: event description
        :return: None
        """
        _s = Storage()
        _s.append_log_history(self._dataset_id, self._id, (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), text))

    @property
    def log_family(self):
        """
        Get current log family
        :return: log family as string
        """
        return self.meta['log_family']

    @log_family.setter
    def log_family(self, family):
        self.meta['log_family'] = family

    def _fetch(self):
        _s = Storage()
        self._values = _s.get_log_data(self._dataset_id, self._id, depth=self.depth, depth__gt=self.depth__gt, depth__lt=self.depth__lt)
        self._meta = _s.get_log_meta(self._dataset_id, self._id)

    def crop(self, depth=None, depth__gt=None, depth__lt=None, inplace=False):
        """
        Method to crop a part of the log.
        :param depth: If only one depth is required else None
        :param depth__gt: Lower boundary of a depth reference
        :param depth__lt: Upper boundary of a depth reference
        :param inplace: It True then edit self object else create a new one in memory
        :return: Log object
        """
        result = self if inplace else copy.deepcopy(self)
        result.depth, result.depth__gt, result.depth_lt = depth, depth__gt, depth__lt
        result._fetch()
        return result

    def validate(self, data):
        """
        Checks if data is a valid value set for BasicLog
        :param data: np.array with value list
        :return: bool or raises exception if assertions don't pass
        """

        # check array has two columns (depth and value)
        assert data.shape[1] == 2, "Data must contain two columns"
        # check depths are unique
        assert len(np.unique(data[:, 0])) == len(data[:, 0]), "All depth references must be unique"

        return True

    def save(self):
        """
        Stores local changes of the log to the database
        :return:
        """
        _s = Storage()
        if self._changes['values']:
            _s.update_logs(self._dataset_id, data={self._id: self._values})
            self._changes['values'] = False
        if self._changes['meta']:
            _s.update_logs(self._dataset_id, meta={self._id: self._meta})
            self._changes['meta'] = False


class MarkersLog(BasicLog):
    _type = 'MarkersLog'

    def validate(self, data):
        # check array has two columns (depth and value)
        assert data.shape[1] == 2, "Data must contain two columns"
        # check depths are unique
        assert len(np.unique(data[:, 0])) == len(data[:, 0]), "All depth references must be unique"

        return True
