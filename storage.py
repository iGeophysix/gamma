import json
import sys
from abc import ABC, abstractmethod

import redis

from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD


class Storage(ABC):
    # DATABASE
    @abstractmethod
    def init_db(self):
        pass

    @abstractmethod
    def flush_db(self):
        pass

    # WELLS
    @abstractmethod
    def list_wells(self):
        pass

    @abstractmethod
    def create_well(self, wellname):
        pass

    @abstractmethod
    def update_well_info(self, wellname, info):
        pass

    @abstractmethod
    def get_well_info(self, wellname):
        pass

    @abstractmethod
    def delete_well(self, wellname):
        pass

    # DATASETS
    @abstractmethod
    def create_dataset(self, wellname, datasetname):
        pass

    @abstractmethod
    def set_dataset_info(self, wellname, datasetname, info):
        pass

    @abstractmethod
    def get_dataset_info(self, wellname, datasetname):
        pass

    @abstractmethod
    def delete_dataset(self, wellname, datasetname):
        pass

    @abstractmethod
    def read_dataset(self, wellname, datasetname, logs=None):
        pass

    @abstractmethod
    def update_dataset(self, wellname, datasetname, data):
        pass

    # LOGS
    @abstractmethod
    def add_log(self, well_name, dataset_name, log_name):
        pass

    @abstractmethod
    def delete_log(self, well_name, dataset_name, log_name):
        pass


class RedisStorage(Storage):
    def __init__(self):
        self.conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

    # DATABASE
    def init_db(self):
        pass

    def flush_db(self):
        self.conn.flushdb()

    def commit(self):
        pass

    # WELLS
    def create_well(self, wellname):
        self.update_well_info(wellname, {})

    def list_wells(self):
        return {well: info for well, info in self.conn.hgetall("wells")}

    def get_well_info(self, wellname):
        return json.loads(self.conn.hget('wells', wellname))

    def update_well_info(self, wellname, info):
        return self.conn.hset("wells", wellname, json.dumps(info))

    def delete_well(self, wellname):
        # delete datasets
        datasets = self.get_well_info(wellname).get("datasets", [])
        for dataset in datasets:
            self.delete_dataset(wellname, dataset)
        self.conn.hdel('wells', wellname)

    def get_datasets(self, wellname):
        wellinfo = self.get_well_info(wellname)
        return wellinfo.get('datasets', [])

    # DATASETS
    @staticmethod
    def __make_dataset_name(wellname, datasetname):
        return f"{wellname}__{datasetname}"

    def create_dataset(self, wellname, datasetname):
        wellinfo = self.get_well_info(wellname)
        if 'datasets' not in wellinfo:
            wellinfo['datasets'] = []
        wellinfo['datasets'].append(datasetname)
        self.update_well_info(wellname, wellinfo)
        return self.__make_dataset_name(wellname, datasetname)

    def set_dataset_info(self, wellname, datasetname, info):
        self.conn.hset('datasets', self.__make_dataset_name(wellname, datasetname), json.dumps(info))

    def get_dataset_info(self, wellname, datasetname):
        return json.loads(self.conn.hget('datasets', self.__make_dataset_name(wellname, datasetname)))

    def delete_dataset(self, wellname, datasetname):
        dataset_id = self.__make_dataset_name(wellname, datasetname)
        self.conn.delete(dataset_id)
        self.conn.hdel('datasets', dataset_id)
        wellinfo = self.get_well_info(wellname)
        wellinfo['datasets'].remove(datasetname)
        self.update_well_info(wellname, wellinfo)

    def read_dataset(self, wellname, datasetname, logs=None, depth=None, depth__gt=None, depth__lt=None):
        dataset_id = self.__make_dataset_name(wellname, datasetname)

        def slice(data, top=None, bottom=None):
            top = top if top is not None else sys.float_info.min
            bottom = bottom if bottom is not None else sys.float_info.max
            return {float(k): v for k, v in data.items() if float(k) >= top and float(k) <= bottom}

        def beautify_depths(data):
            return {float(k): v for k, v in data.items()}

        if logs:
            out = {log: beautify_depths(json.loads(self.conn.hget(dataset_id, log))) for log in logs}
        else:
            out = {k.decode('utf-8'): json.loads(v) for k, v in self.conn.hgetall(dataset_id).items()}

        # apply slicing
        if depth is None and depth__gt is None and depth__lt is None:
            return out
        elif depth__gt is not None or depth__lt is not None:
            return {l: slice(v, depth__gt, depth__lt) for l, v in out.items()}
        else:
            return {l: slice(v, depth, depth) for l, v in out.items()}

    def bulk_load_dataset(self, wellname, datasetname, values: dict) -> None:
        for k, v in values.items():
            self.conn.hset(self.__make_dataset_name(wellname, datasetname), k, json.dumps(v))

    def update_dataset(self, wellname, datasetname, data):
        self.conn.hset(self.__make_dataset_name(wellname, datasetname), mapping=data)

    # LOGS
    def add_log(self, wellname, datasetname, log_name):
        self.conn.hset(self.__make_dataset_name(wellname, datasetname), log_name, '[]')

    def delete_log(self, wellname, datasetname, log_name):
        self.conn.hdel(self.__make_dataset_name(wellname, datasetname), log_name)
