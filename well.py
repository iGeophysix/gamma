import logging
from collections.abc import Iterable

import lasio
import pandas as pd
from sqlalchemy import create_engine

from settings import MISSING_VALUE, META_REFERENCE
from storage import Storage, Storage2, ColumnStorage
from utilities import my_timer

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
        _s = Storage()
        return _s.get_well_info(self._name)

    @info.setter
    def info(self, info):
        _storage = Storage()
        _storage.update_well(self._name, info)
        _storage.commit()

    @property
    def datasets(self):
        _s = Storage()
        datasets = _s.get_well_info(self._name).get('datasets', {}).keys()
        return datasets

    def register_dataset(self, dataset_name, dataset_table_name):
        _s = Storage()
        well_info = _s.get_well_info(self._name)
        dataset_info = self.info['datasets'] if 'datasets' in well_info.keys() else {}
        dataset_info[dataset_name] = dataset_table_name
        well_info['datasets'] = dataset_info
        self.info = well_info

    def unregister_dataset(self, dataset_name):
        _s = Storage()
        well_info = self.info
        well_info['datasets'].pop(dataset_name, None)
        self.info = well_info

    def delete(self):
        for dataset in self.datasets:
            d = WellDataset(self, dataset)
            d.delete()
        _s = Storage()
        _s.delete_well(self._name)
        _s.commit()


class WellDataset:
    def __init__(self, well: Well, name: str) -> None:
        self._well = well.name
        self._name = name
        self._dataset_table_name = self._get_dataset_table_name()

    def _get_dataset_table_name(self):
        _s = Storage()
        return _s.get_well_info(self._well).get('datasets', {}).get(self._name, None)

    def register(self):
        _storage = Storage()
        self._dataset_table_name = _storage.create_dataset(self._well, self._name)
        well = Well(self._well)
        well.register_dataset(self._name, self._dataset_table_name)

    def read_las(self, filename: str):
        debug.debug(f"Reading file: {filename}")
        _storage = Storage()
        well_data = lasio.read(filename)
        self.register()
        temp_df = well_data.df().fillna(MISSING_VALUE)
        _storage.bulk_load_dataset(self._dataset_table_name, reference=temp_df.index, values=temp_df.to_dict('records'))
        _storage.update_dataset(self._dataset_table_name, META_REFERENCE, self.__get_las_headers(well_data.sections))
        _storage.commit()

    def get_data(self, logs=None, start=None, end=None):
        _storage = Storage()
        if start != end:
            result = _storage.read_dataset(self._dataset_table_name, logs, depth__gt=start, depth__lt=end)
        elif start == end:
            result = _storage.read_dataset(self._dataset_table_name, logs, depth=start)
        else:
            result = _storage.read_dataset(self._dataset_table_name)
        return result

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

    @property
    def info(self):
        _storage = Storage()
        return _storage.read_dataset(self._dataset_table_name, depth=META_REFERENCE)[META_REFERENCE]

    @info.setter
    def info(self, info):
        _storage = Storage()
        _storage.update_dataset(self._dataset_table_name, META_REFERENCE, info, autocommit=True)

    def delete(self):
        well = Well(self._well)
        well.unregister_dataset(self._name)
        _s = Storage()
        _s.delete_dataset(self._well, self._name)
        _s.commit()

    def insert(self, reference, row):
        _storage = Storage()
        _storage.update_dataset(self._dataset_table_name, reference, row)

    @my_timer
    def add_curve(self, curve, curve_meta):
        # get initial data
        info = self.info
        data = self.get_data()

        # update data
        df = pd.DataFrame.from_dict(data, orient='index')
        new_data = pd.DataFrame.from_dict(curve, orient='index')
        result = pd.concat([df, new_data], axis=1).fillna(MISSING_VALUE)
        _s = Storage()
        _s.bulk_load_dataset(self._dataset_table_name, reference=result.index, values=result.to_dict('records'))
        engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/test_db")

        # update info
        info['Curves'].update(curve_meta)
        self.info = info

    @my_timer
    def delete_curve(self, curve_name):
        info = self.info
        info['Curves'].pop(curve_name, None)
        _s = Storage()
        _s.delete_curve(self._dataset_table_name, curve_name)
        self.info = info


class WellDataset2:
    def __init__(self, well: Well, name: str) -> None:
        self._well = well.name
        self._name = name
        self._dataset_table_name = self._get_dataset_table_name()

    def _get_dataset_table_name(self):
        _s = Storage2()
        return _s.get_well_info(self._well).get('datasets', {}).get(self._name, None)

    def register(self):
        _storage = Storage2()
        self._dataset_table_name = _storage.create_dataset(self._well, self._name)
        well = Well(self._well)
        well.register_dataset(self._name, self._dataset_table_name)

    def read_las(self, filename: str):
        debug.debug(f"Reading file: {filename}")
        _storage = Storage2()
        well_data = lasio.read(filename)
        self.register()
        temp_df = well_data.df().fillna(MISSING_VALUE)
        _storage.bulk_load_dataset(self._dataset_table_name, reference=temp_df.columns, values=temp_df.to_dict())
        _storage.update_dataset(self._dataset_table_name, META_REFERENCE, self.__get_las_headers(well_data.sections))
        _storage.commit()

    def get_data(self, logs=None, start=None, end=None):
        _storage = Storage2()
        if start != end:
            result = _storage.read_dataset(self._dataset_table_name, logs, depth__gt=start, depth__lt=end)
        elif start == end:
            result = _storage.read_dataset(self._dataset_table_name, logs, depth=start)
        elif start is None and end is None:
            result = _storage.read_dataset(self._dataset_table_name)
        return result

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

    @property
    def info(self):
        _storage = Storage2()
        return _storage.read_dataset(self._dataset_table_name, logs=[META_REFERENCE, ])[str(META_REFERENCE)]

    @info.setter
    def info(self, info):
        _storage = Storage2()
        _storage.update_dataset(self._dataset_table_name, META_REFERENCE, info, autocommit=True)

    def delete(self):
        well = Well(self._well)
        well.unregister_dataset(self._name)
        _s = Storage2()
        _s.delete_dataset(self._well, self._name)
        _s.commit()

    def insert(self, reference, row):
        _storage = Storage2()
        _storage.update_dataset(self._dataset_table_name, reference, row)

    @my_timer
    def add_curve(self, curve, curve_meta):
        # get initial data
        info = self.info
        data = self.get_data()

        # update data
        df = pd.DataFrame.from_dict(data, orient='index')
        new_data = pd.DataFrame.from_dict(curve, orient='index')
        result = pd.concat([df, new_data], axis=1).fillna(MISSING_VALUE)
        _s = Storage2()
        _s.bulk_load_dataset(self._dataset_table_name, reference=result.index, values=result.to_dict('records'))

        # update info
        info['Curves'].update(curve_meta)
        self.info = info

    @my_timer
    def delete_curve(self, curve_name):
        info = self.info
        info['Curves'].pop(curve_name, None)
        _s = Storage2()
        _s.delete_curve(self._dataset_table_name, curve_name)
        self.info = info


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
        well = Well(self._well)
        well.register_dataset(self._name, self._dataset_table_name)

    def read_las(self, filename: str):
        debug.debug(f"Reading file: {filename}")
        _storage = ColumnStorage()
        well_data = lasio.read(filename)
        self.register()
        temp_df = well_data.df().fillna(MISSING_VALUE)
        values = temp_df.to_dict('index')
        logs = {log: float for log in temp_df.columns}
        _storage.bulk_load_dataset(well_name=self._well, dataset_name=self._name, logs=logs, values=values)
        # _storage.update_dataset(self._dataset_table_name, META_REFERENCE, self.__get_las_headers(well_data.sections))
        # _storage.commit()

    def delete(self):
        _s = ColumnStorage()
        well = Well(self._well)
        well.unregister_dataset(self._name)
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
            raise Exception("Log name is neither a str nor an iterable")

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
