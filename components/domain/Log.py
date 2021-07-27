import copy
import hashlib
import json
import warnings
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d

from components.database.RedisStorage import RedisStorage as Storage
from components.importexport.UnitsSystem import UnitsSystem
from utilities import safe_run, dict_replace_value


class BasicLog:
    """
    General class for all types of logs
    """

    def __init__(self, dataset_id: str = None, log_id: str = 'New Log'):
        """
        Init method
        :param dataset_id: Parent dataset_id
        :param log_id: Log name
        """
        self._meta = BasicLogMeta(dataset_id, log_id)
        self.dataset_id = dataset_id
        self._id = log_id
        self._type = 'BasicLog'
        self._values = None
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

    def __getitem__(self, key, ) -> np.array:
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
        return _s.check_log_exists(self.dataset_id, self._id)

    @property
    def name(self) -> str:
        """
        Returns log name from meta
        :return:
        """
        return self.meta.name

    @name.setter
    def name(self, name: str) -> None:
        """
        Set log name in meta
        :return:
        """
        self.meta.name = name

    @property
    def dataset_id(self) -> str:
        """
        Get current dataset id
        :return: dataset id as string
        """
        return self._meta.dataset_id

    @dataset_id.setter
    def dataset_id(self, dataset_id) -> None:
        """Assign dataset id to the log"""
        self._meta.dataset_id = dataset_id

    @property
    def values(self) -> np.array:
        """
        Get log values
        :return: np.array with log values
        """
        s = Storage()
        if self._values is None and s.check_log_values_exists(self.dataset_id, self._id):
            self._fetch()
        return self._values

    @staticmethod
    def __remove_nans(values):
        """
        Removes rows with missing values from the top and bottom till the first non-null value
        :param values:
        :return:
        """
        for i, val in enumerate(values[:, 1]):
            if val == val:  # check if is nan
                values = values[i:, :]
                break

        for i, val in enumerate(reversed(values[:, 1])):
            if val == val:
                bound = -i if i else None
                values = values[:bound, :]
                break

        return values if values.size else np.array((0, np.nan))

    @values.setter
    def values(self, values: np.array) -> None:
        """
        Set log values
        :param values: np.array with data
        :return: None
        """
        if not self.validate(values):
            raise ValueError("Data is not passing validation")
        self._values = copy.deepcopy(values)
        self._changes['values'] = True
        self._meta.basic_statistics = self.get_basic_curve_statistics(self._values)
        self.update_hashes()

    def convert_units(self, units_to: str) -> np.array:
        """
        Function that returns the log in another units
        :param units_to: units to convert to. Must be known in the UnitsSystem dictionary
        :return: converted values as np.array
        """
        converter = UnitsSystem()
        data = self.values[:, 1]
        units_from = self.meta.units
        converted_values = np.vstack([self.values[:, 0], converter.convert(data, units_from, units_to)]).T
        return converted_values

    def interpolate(self, new_reference: np.array):
        """
        Interpolates log values to new depth reference
        :param new_reference: new reference as np.array
        :return: log values
        """
        new_values = interp1d(self.values[:, 0], self.values[:, 1], bounds_error=False, fill_value=np.nan)(new_reference)
        return np.vstack((new_reference, new_values)).T

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
    def meta(self):
        """
        Get log meta information
        :return: dictionary with log meta information
        """
        return self._meta

    @meta.setter
    def meta(self, meta_info: dict) -> None:
        """
        Set log meta information
        :param meta: new meta as dictionary
        :return: None
        """
        if type(meta_info) == BasicLogMeta:
            self._meta = meta_info
        else:
            self._meta = BasicLogMeta(self.dataset_id, self._id)
            self._meta.__ior__(meta_info)
        self.update_hashes()
        self._changes['meta'] = True

    @staticmethod
    def md5(text) -> str:
        return str(hashlib.md5(text).hexdigest())

    @property
    def full_hash(self) -> str:
        """
        Get hash value of the log data and meta
        :return: str
        """
        return self._meta.full_hash

    @property
    def data_hash(self) -> str:
        """
        Get hash value of the log data
        :return: str
        """
        return self._meta.data_hash

    @property
    def meta_hash(self) -> str:
        """
        Get hash value of the log meta
        :return: str
        """
        return self._meta.meta_hash

    def update_hashes(self):
        """
        Update hash values for data and meta info of the log
        :return:
        """
        data_as_string = self.values.tobytes() if self.values is not None else b'MissingValue'

        data_hash = self.md5(data_as_string)
        self._meta.data_hash = data_hash
        self._meta.full_hash = data_hash + self._meta.meta_hash

    def _fetch(self):
        _s = Storage()
        self._values = _s.get_log_data(self.dataset_id, self._id, depth=self.depth, depth__gt=self.depth__gt, depth__lt=self.depth__lt)
        self._meta.__ior__(_s.get_log_meta(self.dataset_id, self._id))

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

    @staticmethod
    def validate(data):
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
            self._meta.basic_statistics = self.get_basic_curve_statistics(self._values)
            _s.update_logs(self.dataset_id, data={self._id: self._values})
            self._changes['values'] = False

        self._meta.save()

    @staticmethod
    def get_basic_curve_statistics(log_data: np.array) -> dict:
        '''
        Returns basic stats for a log (np.array)
        :param log_data: input data as np.array
        :return: dict with basic stats (interval of not NaN values, mean, gmean, etc)
        '''

        def geo_mean(iterable):
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                a = np.log(iterable)
                return np.exp(a.mean())

        non_null_values = log_data[~pd.isnull(log_data[:, 1])]
        if non_null_values.shape[0] == 0:  # if all values are nan
            return {}

        min_depth = safe_run(np.min)(non_null_values[:, 0])
        max_depth = safe_run(np.max)(non_null_values[:, 0])
        depth_span = max_depth - min_depth if max_depth is not None and min_depth is not None else None
        min_value = safe_run(np.min)(non_null_values[:, 1])
        max_value = safe_run(np.max)(non_null_values[:, 1])
        mean = safe_run(np.mean)(non_null_values[:, 1])
        log_gmean = safe_run(geo_mean)(non_null_values[:, 1])
        stdev = safe_run(np.std)(non_null_values[:, 1])
        derivative = safe_run(np.diff)(log_data[:, 0])
        const_step = bool(abs(derivative.min() - derivative.max()) < 0.00001) if derivative is not None else None
        avg_step = derivative.mean() if derivative is not None else None
        new_meta = {"min_depth": min_depth,
                    "max_depth": max_depth,
                    "min_value": min_value,
                    "max_value": max_value,
                    "depth_span": depth_span,
                    "avg_step": avg_step,
                    "const_step": const_step,
                    "mean": mean,
                    "gmean": log_gmean,
                    "stdev": stdev}
        return new_meta


@dataclass
class BasicLogMeta:
    """
    Class to manage log meta
    """
    dataset_id: str
    log_id: str
    name: str
    tags: list
    history: list
    units: str
    type: str = 'BasicLog'
    depth_reference: str = 'MD'

    def __init__(self, dataset_id, log_id):
        self.dataset_id = dataset_id
        self.log_id = log_id

        self.name = self.log_id
        self.tags = list()
        self.history = []
        self.units = ""

        _s = Storage()
        if _s.check_log_exists(self.dataset_id, self.log_id):
            self.load()

    def __ior__(self, other):
        for name, value in other.items():
            self.__setattr__(name, value)

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __contains__(self, item):
        return hasattr(self, item)

    def update(self, other):
        self.__ior__(other)

    def add_tags(self, *args):
        self.tags = list(set(self.tags + list(args)))

    def delete_tags(self, *args):
        for tag in args:
            self.tags.remove(tag)

    def append_history(self, history):
        # self.history.append((datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), history))
        self.history.append((datetime.now().isoformat(), history))

    def load(self):
        _s = Storage()
        meta_items = _s.get_log_meta(self.dataset_id, self.log_id)
        for name, value in meta_items.items():
            name = name.replace(f"_{self.__class__.__name__}", "")
            self.__setattr__(name, value)

    def save(self):
        _s = Storage()
        _s.update_logs(self.dataset_id, meta={self.log_id: self.asdict()})

    def asdict(self):
        out = {key: self.__getattribute__(key) for key in dir(self) if not key.startswith('_') and not callable(self.__getattribute__(key))}
        out = dict_replace_value(out, np.nan, None)
        return out

    def md5(self):
        excluded_meta_fields = ["data_hash", "meta_hash", "full_hash", "history"]
        meta = {key: self[key] for key in dir(self) if key not in excluded_meta_fields and not key.startswith('__') and not callable(self.__getattribute__(key))}
        meta_hash = hashlib.md5(json.dumps(meta).encode()).hexdigest()
        return meta_hash

    @property
    def meta_hash(self):
        return self.md5()

    # this method is required to avoid problems on loading
    @meta_hash.setter
    def meta_hash(self, val):
        pass

class MarkersLog(BasicLog):

    def __init__(self, dataset_id: str = None, log_id: str = 'New Log'):
        super().__init__(dataset_id=dataset_id, log_id=log_id)
        self._type = 'MarkersLog'
        self.meta.type = 'MarkersLog'

    def validate(self, data):
        # check array has two columns (depth and value)
        assert data.shape[1] == 2, "Data must contain two columns"
        # check depths are unique
        assert len(np.unique(data[:, 0])) == len(data[:, 0]), "All depth references must be unique"

        return True
