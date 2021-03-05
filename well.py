import logging
from collections.abc import Iterable

import lasio

from settings import MISSING_VALUE
from storage import ColumnStorage

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
        _s = ColumnStorage()
        return _s.get_well_info(self._name)

    @info.setter
    def info(self, info):
        _storage = ColumnStorage()
        _storage.update_well_info(self._name, info)
        _storage.commit()

    @property
    def datasets(self):
        _s = ColumnStorage()
        datasets = _s.get_datasets(self._name).keys()
        return datasets

    def delete(self):
        for dataset in self.datasets:
            d = WellDatasetColumns(self, dataset)
            d.delete()
        _s = ColumnStorage()
        _s.delete_well(self._name)
        _s.commit()


class WellDatasetColumns:
    def __init__(self, well: Well, name: str) -> None:
        self._well = well.name
        self._name = name
        self._dataset_table_name = self._get_dataset_table_name()

    def _get_dataset_table_name(self):
        _s = ColumnStorage()
        return _s.get_well_info(self._well).get('datasets', {}).get(self._name, None)

    def register(self):
        _storage = ColumnStorage()
        self._dataset_table_name = _storage.create_dataset(self._well, self._name)

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
        _storage = ColumnStorage()
        well_data = lasio.read(filename)
        self.register()
        temp_df = well_data.df().fillna(MISSING_VALUE)
        values = temp_df.to_dict('index')
        logs = {log: float for log in temp_df.columns}
        _storage.bulk_load_dataset(well_name=self._well, dataset_name=self._name, logs=logs, values=values, autocommit=False)
        _storage.set_dataset_info(self._well, self._name, self.__get_las_headers(well_data.sections), autocommit=False)
        _storage.commit()

    def delete(self):
        _s = ColumnStorage()
        _s.delete_dataset(self._well, self._name)
        _s.commit()

    def add_log(self, name, dtype):
        _s = ColumnStorage()
        if type(name) == str:
            _s.add_log(self._well, self._name, log_name=name, log_type=_s.get_data_type(dtype))
        elif isinstance(name, Iterable):
            for n, d in zip(name, dtype):
                _s.add_log(self._well, self._name, log_name=n, log_type=_s.get_data_type(d), autocommit=False)
            _s.commit()
        else:
            raise TypeError("Log wellname is neither a str nor an iterable")

    def delete_log(self, name):
        _s = ColumnStorage()
        _s.delete_log(self._well, self._name, log_name=name)

    def get_data(self, logs=None, start=None, end=None):
        _storage = ColumnStorage()
        if start != end:
            result = _storage.read_dataset(self._well, self._name, logs, depth__gt=start, depth__lt=end)
        elif start == end:
            result = _storage.read_dataset(self._well, self._name, logs, depth=start)
        else:
            result = _storage.read_dataset(self._well, self._name)
        return result

    def insert(self, data):
        _storage = ColumnStorage()
        _storage.update_dataset(self._well, self._name, data)
