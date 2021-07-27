import hashlib

from components.database.RedisStorage import RedisStorage
from components.domain.WellDataset import WellDataset


class Well:
    """
    Class to process manipulations with wells
    """

    def __init__(self, name: str, new=False):
        """
        Initialization of the object
        :param name: well name
        :param new: False by default. If True - it will create a new well in the data storage
        """
        self._name = name
        if new and not self.exists():
            _s = RedisStorage()
            _s.create_well(wellname=self._name)

    def __str__(self):
        """
        To use Well object in print or strings
        :return: string representation of the Well objet
        """
        return self._name

    @property
    def id(self):
        return RedisStorage._get_well_id(self._name)

    def exists(self) -> bool:
        """
        Check if well exists in db
        :return: True or False
        """
        return RedisStorage().check_well_exists(self._name)

    @property
    def name(self):
        """
        Returns well name as property
        :return: Well name
        """
        return self._name

    @property
    def meta(self) -> dict:
        """
        Returns well meta information
        :return: dict
        """
        _s = RedisStorage()
        return _s.get_well_info(self._name)

    @meta.setter
    def meta(self, info):
        """
        Sets new meta information.
        :param info: Complete dictionary with the new information
        """
        _storage = RedisStorage()
        _storage.set_well_info(self._name, info)

    def update_meta(self, info):
        '''
        Update well meta
        :param info:
        :return:
        '''
        _storage = RedisStorage()
        current_info = _storage.get_well_info(self._name)
        current_info |= info
        _storage.set_well_info(self._name, current_info)

    def md5(self):
        dataset_hashes = []
        for ds_name in self.datasets:
            ds = WellDataset(self, ds_name)
            dataset_hashes.append(ds.md5())

        md5 = hashlib.md5(str((tuple(sorted(dataset_hashes)), self.meta)).encode()).hexdigest()
        return md5

    @property
    def datasets(self):
        """
        Returns list of datasets with readable names
        :return:
        """
        _s = RedisStorage()
        return _s.get_datasets(self._name)

    def delete(self):
        """
        Deletes the well with its datasets and logs
        """
        _s = RedisStorage()
        _s.delete_well(self._name)


def get_well_by_id(well_id: str) -> Well:
    """
    Get Well by well_id
    :param well_id: str
    :return: Well object
    """
    _s = RedisStorage()
    wellname = _s.get_well_name_by_id(well_id)
    return Well(wellname)


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