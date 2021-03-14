import logging

from datetime import datetime
from typing import Any

from database.RedisStorage import RedisStorage
from domain.Well import Well
from importexport import las

logging.basicConfig()
debug = logging.getLogger("petrotool")
debug.setLevel(logging.INFO)


class WellDataset:
    """
    Class to process manipulations with datasets and its logs
    """

    def __init__(self, well: Well, name: str, new=False) -> None:
        """
        Initialize Dataset object
        :param well: Well - object itself, not wellname
        :param name: Dataset name - string
        :param new: bool value, default is False. If true, a new dataset will be created in the data storage
        """
        self._well = well.name
        self._name = name
        self._dataset_table_name = None
        if new:
            self.register()

    def delete(self) -> None:
        """
        Delete dataset and it contents, also remove it from well
        :return:
        """
        _s = RedisStorage()
        _s.delete_dataset(self._well, self._name)

    def register(self) -> None:
        """
        Register dataset in the storage
        :return:
        """
        _storage = RedisStorage()
        self._dataset_table_name = _storage.create_dataset(self._well, self._name)

    @property
    def info(self) -> dict:
        """
        Get dataset meta information
        :return: dict
        """
        _s = RedisStorage()
        return _s.get_dataset_info(self._well, self._name)['meta']

    @info.setter
    def info(self, info: dict) -> None:
        """
        Set meta info in the dataset. Completly rewrites previous contents.
        You should always send whole contents, not only updated parts
        :param info:
        """
        _s = RedisStorage()
        _s.set_dataset_info(self._well, self._name, info)

    def read_las(self, filename: str) -> dict:
        """
        Batch job to read data from a file.
        :param filename: file path should be accessible via os.path
        :return: well info from las file header
        """
        debug.debug(f"Reading file: {filename}")
        _storage = RedisStorage()
        well_data = las.parse_las_file(filename)
        values = well_data.to_dict()
        well_info = well_data.well_info()

        _storage.update_logs(wellname=self._well, datasetname=self._name, data=values, meta=well_data.logs_info())
        for log in values.keys():
            _storage.append_log_history(wellname=self._well, datasetname=self._name, log=log, event=(datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), f"Loaded from {filename}"))
        _storage.set_dataset_info(self._well, self._name, well_info)
        return well_info

    def get_log_list(self) -> list:
        """
        Returns list of logs available in the dataset
        :return: list
        """
        _storage = RedisStorage()
        return _storage.get_dataset_logs(self._well, self._name)

    def delete_log(self, name: str) -> None:
        """
        Delete one log from the dataset
        :param name: name of the log
        """
        _s = RedisStorage()
        _s.delete_log(self._well, self._name, log_name=name)

    def get_log_data(self, logs: list = None, start: float = None, end: float = None) -> dict[dict]:
        """
        Reads data from the storage.
        :param logs: list of logs to get, if None - return all logs
        :param start: start depth (signed), if None - returns from the least value (> -inf)
        :param end: end depth (signed), if None - returns till the largest value (< +inf)
        :return: dict of dicts with each log and its data: depth reference-value
            Example: {"GR": {10.0: 1, 20.0: 2}, "PS": {10.0: 3, 20.0: 4}}
        """
        _storage = RedisStorage()
        if start != end:
            result = _storage.get_logs_data(self._well, self._name, logs, depth__gt=start, depth__lt=end)
        elif start == end:
            result = _storage.get_logs_data(self._well, self._name, logs, depth=start)
        else:
            result = _storage.get_logs_data(self._well, self._name)
        return result

    def get_log_meta(self, logs: list = None) -> dict[dict]:
        """
        Reads logs' meta data from the storage.
        :param logs: list of logs (or any other iterable)
        :return: dict with logs meta data
            Example: {"GR": {"units": "gAPI", "code": "", "description": "GR"}, "PS": {"units": "uV", "code": "", "description": "PS"}}
        """
        _storage = RedisStorage()
        return _storage.get_logs_meta(self._well, self._name, logs)

    def get_log_history(self, log: str) -> list:
        """
        Reads history of one log
        :param log: log name (str)
        :return: list of events
        """
        _s = RedisStorage()
        return _s.get_logs_meta(self._well, self._name, [log, ])[log].get('__history', [])

    def append_log_history(self, log: str, event: Any) -> None:
        """
        Appends one more event to the tail of log history data
        :param log: log name (str)
        :param event: event - any serializable object

        """
        _s = RedisStorage()
        _s.append_log_history(self._well, self._name, log, event)

    def set_data(self, data: dict[dict] = None, meta: dict[dict] = None) -> None:
        """
        Set log data and meta-information.
        :param data: dict with {log: {depth:value,...},...}, if None then data won't be updated
        :param meta: dict with {log: {key:value,...},...}, if None then meta won't be updated
        """
        _storage = RedisStorage()
        _storage.update_logs(self._well, self._name, data, meta)
