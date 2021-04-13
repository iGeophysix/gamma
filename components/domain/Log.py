import copy
from datetime import datetime

import numpy as np

from components.database.RedisStorage import RedisStorage as Storage


class BasicLog:
    """
    General class for all types of logs
    """

    def __init__(self, dataset_id: str, name, ):
        """
        Init method
        :param dataset_id: Parent dataset_id
        :param name: Log name
        """
        self._dataset_id = dataset_id
        self._name = name
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
        return _s.check_log_exists(self._dataset_id, self._name)

    @property
    def name(self) -> str:
        """
        Returns log name
        :return:
        """
        return self._name

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

    @property
    def meta(self) -> dict:
        """
        Get log meta information
        :return: dictionary with log meta information
        """
        _s = Storage()
        self._meta = _s.get_log_meta(self._dataset_id, self._name)
        return self._meta

    @meta.setter
    def meta(self, meta: dict) -> None:
        """
        Set log meta information
        :return: None
        """
        self._meta = meta
        self._meta["__type"] = self._type
        self._changes['meta'] = True

    @property
    def history(self) -> list[tuple]:
        """
        Get whole history of the log curve
        :return:
        """
        _s = Storage()
        return _s.log_history(self._dataset_id, self._name)

    @history.setter
    def history(self, text) -> None:
        """
        Appends an event to history
        :param text: event description
        :return: None
        """
        _s = Storage()
        _s.append_log_history(self._dataset_id, self._name, (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), text))

    def _fetch(self):
        _s = Storage()
        self._values = _s.get_log_data(self._dataset_id, self._name, depth=self.depth, depth__gt=self.depth__gt, depth__lt=self.depth__lt)
        self._meta = _s.get_log_meta(self._dataset_id, self._name)

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
            _s.update_logs(self._dataset_id, data={self._name: self._values})
            self._changes['values'] = False
        if self._changes['meta']:
            _s.update_logs(self._dataset_id, meta={self._name: self._meta})
            self._changes['meta'] = False


class MarkersLog(BasicLog):
    _type = 'MarkersLog'

    def validate(self, data):
        # check array has two columns (depth and value)
        assert data.shape[1] == 2, "Data must contain two columns"
        # check depths are unique
        assert len(np.unique(data[:, 0])) == len(data[:, 0]), "All depth references must be unique"

        return True