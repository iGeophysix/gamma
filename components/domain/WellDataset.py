import logging
from datetime import datetime

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well

logging.basicConfig()
debug = logging.getLogger("welldataset")
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
        logging.info(f"Deleted dataset {self._name} in well {self._well}")
        self._s.delete_dataset(self._well, self._name)

    def register(self) -> None:
        """
        Register dataset in the storage
        :return:
        """
        logging.info(f"Created dataset {self._name} in well {self._well}")
        self._dataset_table_name = self._s.create_dataset(self._well, self._name)

    @property
    def well(self) -> str:
        """
        Return parent well name
        :return:
        """
        return self._well

    @property
    def name(self) -> str:
        """
        Return self name
        :return:
        """
        return self._name

    @property
    def id(self) -> str:
        return RedisStorage()._get_dataset_id(self._well, self._name)

    @property
    def info(self) -> dict:
        """
        Get dataset meta information
        :return: dict
        """
        return self._s.get_dataset_info(dataset_id=self.id)['meta']

    @info.setter
    def info(self, info: dict) -> None:
        """
        Set meta info in the dataset. Completly rewrites previous contents.
        You should always send whole contents, not only updated parts
        :param info:
        """
        logging.debug(f"Changed dataset info {self._name} in well {self._well}")
        self._s.set_dataset_info(self.id, info)

    @property
    def log_list(self):
        return self._s.get_dataset_logs(self.id)

    def get_log_list(self, **kwargs) -> list:
        """
        Returns list of logs available in the dataset
        :params kwargs: optional field to filter list of logs by meta attributes e.g. mean=0.5, log_family='Gamma Ray', min_depth__lt=1000
        :return: list
        """
        if kwargs is None:
            return self._s.get_dataset_logs(self.id)

        logs_meta = self._s.get_logs_meta(self.id)
        to_delete = []
        for log in logs_meta.keys():
            for key, value in kwargs.items():
                try:
                    if key.endswith('__lt'):
                        k = key[:-4]
                        if logs_meta[log][k] >= value:
                            to_delete.append(log)
                    elif key.endswith('__gt'):
                        k = key[:-4]
                        if logs_meta[log][k] <= value:
                            to_delete.append(log)

                    else:
                        if logs_meta[log][key] != value:
                            to_delete.append(log)
                except KeyError:
                    to_delete.append(log)

        return [log for log in logs_meta.keys() if log not in to_delete]

    def delete_log(self, name: str) -> None:
        """
        Delete one log from the dataset
        :param name: name of the log
        """
        logging.info(f"Deleted log {name} in dataset {self._name} in well {self._well}")
        self._s.delete_log(self.id, log_name=name)

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
        if logs is None:
            logs = self.get_log_list()
        if start != end:
            result = {log: BasicLog(self.id, log).crop(depth__gt=start, depth__lt=end, inplace=True) for log in logs}
        elif start == end and start and end:
            result = {log: BasicLog(self.id, log).crop(depth=start, inplace=True) for log in logs}
        else:
            result = {log: BasicLog(self.id, log) for log in logs}

        return result

    def get_log_meta(self, logs: list = None) -> dict[dict]:
        """
        Reads logs' meta data from the storage.
        :param logs: list of logs (or any other iterable)
        :return: dict with logs meta data
            Example: {"GR": {"units": "gAPI", "code": "", "description": "GR"}, "PS": {"units": "uV", "code": "", "description": "PS"}}
        """
        return self._s.get_logs_meta(self.id, logs)

    def get_log_history(self, log: str) -> list:
        """
        Reads history of one log
        :param log: log name (str)
        :return: list of events
        """
        return self._s.get_logs_meta(self.id, [log, ])[log].get('__history', [])

    def append_log_history(self, log: str, event: str) -> None:
        """
        Appends one more event to the tail of log history data. It will automatically add datetime stamp in the event
        :param log: log name (str)
        :param event: event - text as string

        """
        logging.debug(f"Added history event to dataset {self._name} in well {self._well}")
        self._s.append_log_history(self.id, log, (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), event))

    def set_data(self, data: dict[str, np.ndarray] = None, meta: dict[str, dict] = None) -> None:
        """
        Set log data and meta-information.
        :param data: dict with {log: {depth:value,...},...}, if None then data won't be updated
        :param meta: dict with {log: {key:value,...},...}, if None then meta won't be updated
        """

        logging.info(f"Set data and/or metadata in dataset {self._name} in well {self._well}")

        self._s.update_logs(self.id, data, meta)

    def append_log_meta(self, meta: dict[str, dict]) -> None:
        """
        Appends (or updates if key exists) meta information in the log
        :param meta: dictionary with log names as keys and dicts with new meta information
        Example:
            d = WellDataset(wname,dname)
            d.append_log_meta({"GR":{"field1":1, "field2":"two"}})
        """
        logging.info(f"Append meta to logs {list(meta.keys())} dataset {self._name} in well {self._well}")
        for log, data in meta.items():
            self._s.append_log_meta(self.id, log, data)
