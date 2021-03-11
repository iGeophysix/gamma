import logging

import lasio

from storage import RedisStorage

logging.basicConfig()
debug = logging.getLogger("petrotool")
debug.setLevel(logging.INFO)


class Well:
    def __init__(self, name: str, new=False):
        self._name = name
        if new:
            self.info = {}

    def __str__(self):
        return self._name

    @property
    def name(self):
        return self._name

    @property
    def info(self):
        _s = RedisStorage()
        return _s.get_well_info(self._name)

    @info.setter
    def info(self, info):
        _storage = RedisStorage()
        _storage.update_well_info(self._name, info)

    @property
    def datasets(self):
        _s = RedisStorage()
        return _s.get_datasets(self._name)

    def delete(self):
        _s = RedisStorage()
        _s.delete_well(self._name)


class WellDataset:
    def __init__(self, well: Well, name: str) -> None:
        self._well = well.name
        self._name = name
        self._dataset_table_name = None

    def delete(self):
        _s = RedisStorage()
        _s.delete_dataset(self._well, self._name)

    def register(self):
        _storage = RedisStorage()
        self._dataset_table_name = _storage.create_dataset(self._well, self._name)

    @property
    def info(self):
        _s = RedisStorage()
        return _s.get_dataset_info(self._well, self._name)

    @info.setter
    def info(self, info):
        _s = RedisStorage()
        return _s.set_dataset_info(self._well, self._name, info)

    @staticmethod
    def __get_las_headers(sections, keys=None, exclude=('data', 'json')):
        def section_to_dict(section, keys=None, exclude=('data', 'json')):
            if type(section) == str:
                return section

            _out = {}
            for field, item in section.items():
                _out[field] = {}
                if keys is None:
                    keys = [key for key in item.__dict__.keys() if key not in exclude]
                _out[field].update({key: item[key] for key in keys})

            return _out

        return {section: section_to_dict(sections[section], keys, exclude) for section in sections}

    def read_las(self, filename: str):
        debug.debug(f"Reading file: {filename}")
        _storage = RedisStorage()
        well_data = lasio.read(filename)
        self.register()
        temp_df = well_data.df()
        values = temp_df.to_dict()
        _storage.bulk_load_dataset(wellname=self._well, datasetname=self._name, values=values)
        _storage.set_dataset_info(self._well, self._name, self.__get_las_headers(well_data.sections))

    def delete_log(self, name):
        _s = RedisStorage()
        _s.delete_log(self._well, self._name, log_name=name)

    def get_data(self, logs=None, start=None, end=None):
        _storage = RedisStorage()
        if start != end:
            result = _storage.read_dataset(self._well, self._name, logs, depth__gt=start, depth__lt=end)
        elif start == end:
            result = _storage.read_dataset(self._well, self._name, logs, depth=start)
        else:
            result = _storage.read_dataset(self._well, self._name)
        return result

    def set_data(self, data):
        _storage = RedisStorage()
        _storage.update_dataset(self._well, self._name, data)
