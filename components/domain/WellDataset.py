import logging
from typing import Any

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well

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
        self._s = RedisStorage()
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
        self._s.delete_dataset(self._well, self._name)

    def register(self) -> None:
        """
        Register dataset in the storage
        :return:
        """
        self._dataset_table_name = self._s.create_dataset(self._well, self._name)

    @property
    def info(self) -> dict:
        """
        Get dataset meta information
        :return: dict
        """
        return self._s.get_dataset_info(self._well, self._name)['meta']

    @info.setter
    def info(self, info: dict) -> None:
        """
        Set meta info in the dataset. Completly rewrites previous contents.
        You should always send whole contents, not only updated parts
        :param info:
        """
        self._s.set_dataset_info(self._well, self._name, info)

    def get_log_list(self) -> list:
        """
        Returns list of logs available in the dataset
        :return: list
        """
        return self._s.get_dataset_logs(self._well, self._name)

    def delete_log(self, name: str) -> None:
        """
        Delete one log from the dataset
        :param name: name of the log
        """
        self._s.delete_log(self._well, self._name, log_name=name)

    def get_log_data(self,
                     logs: list = None,
                     start: float = None,
                     end: float = None):
        """
        Reads data from the storage.
        :param logs: list of logs to get, if None - return all logs
        :param start: start depth (signed), if None - returns from the least value (> -inf)
        :param end: end depth (signed), if None - returns till the largest value (< +inf)
        :return: dict of dicts with each log and its data: depth reference-value
            Example: {"GR": {10.0: 1, 20.0: 2}, "PS": {10.0: 3, 20.0: 4}}
        """
        if start != end:
            result = self._s.get_logs_data(self._well,
                                           self._name,
                                           logs,
                                           depth__gt=start,
                                           depth__lt=end)
        elif start == end:
            result = self._s.get_logs_data(self._well,
                                           self._name,
                                           logs,
                                           depth=start)
        else:
            result = self._s.get_logs_data(self._well,
                                           self._name)
        return result

    def get_log_meta(self, logs: list = None) -> dict[dict]:
        """
        Reads logs' meta data from the storage.
        :param logs: list of logs (or any other iterable)
        :return: dict with logs meta data
            Example: {"GR": {"units": "gAPI", "code": "", "description": "GR"}, "PS": {"units": "uV", "code": "", "description": "PS"}}
        """
        return self._s.get_logs_meta(self._well, self._name, logs)

    def get_log_history(self, log: str) -> list:
        """
        Reads history of one log
        :param log: log name (str)
        :return: list of events
        """
        return self._s.get_logs_meta(self._well, self._name, [log, ])[log].get('__history', [])

    def append_log_history(self, log: str, event: Any) -> None:
        """
        Appends one more event to the tail of log history data
        :param log: log name (str)
        :param event: event - any serializable object

        """
        self._s.append_log_history(self._well, self._name, log, event)

    def set_data(self, data=None, meta: dict[dict] = None) -> None:
        """
        Set log data and meta-information.
        :param data: dict with {log: {depth:value,...},...}, if None then data won't be updated
        :param meta: dict with {log: {key:value,...},...}, if None then meta won't be updated
        """
        self._s.update_logs(self._well, self._name, data, meta)

    def append_log_meta(self, meta: dict[Any]) -> None:
        for log, data in meta.items():
            self._s.append_log_meta(self._well, self._name, log, data)
