from components.database.RedisStorage import RedisStorage

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
        if new:
            _s = RedisStorage()
            _s.create_well(wellname=self._name)

    def __str__(self):
        """
        To use Well object in print or strings
        :return: string representation of the Well objet
        """
        return self._name

    @property
    def name(self):
        """
        Returns well name as property
        :return: Well name
        """
        return self._name

    @property
    def info(self) -> dict:
        """
        Returns well meta information
        :return: dict
        """
        _s = RedisStorage()
        return _s.get_well_info(self._name)

    @info.setter
    def info(self, info):
        """
        Sets new meta information.
        :param info: Complete dictionary with the new information
        """
        _storage = RedisStorage()
        _storage.set_well_info(self._name, info)

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