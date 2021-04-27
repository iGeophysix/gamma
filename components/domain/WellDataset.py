import logging

from components.database.RedisStorage import RedisStorage
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

    @property
    def exists(self) -> bool:
        """
        Check if the log is exists in the dataset
        :return: bool
        """
        _s = RedisStorage()
        return _s.check_dataset_exists(self.id)

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
        """
        Get dataset id
        :return:
        """
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
    def log_list(self) -> list[str]:
        """
        Get full log list
        :return:
        """
        return self._s.get_dataset_logs(self.id)

    def get_log_list(self, **kwargs) -> list:
        """
        Returns list of logs available in the dataset
        :params kwargs: optional field to filter list of logs by meta attributes e.g. mean=0.5, family='Gamma Ray', min_depth__lt=1000
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


def get_dataset_by_id(dataset_id: str) -> WellDataset:
    """
    Get Well Dataset by dataset id
    :param dataset_id: str
    :return: WellDataset object
    """
    s = RedisStorage()
    dataset_meta = s.get_dataset_info(dataset_id)
    well = Well(dataset_meta['well_name'])
    return WellDataset(well, dataset_meta['name'])
