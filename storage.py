import hashlib
import json
import logging
import sys

import redis

from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

logger = logging.getLogger("storage")

BLOCKING_TIMEOUT = 5


class RedisStorage:
    def __init__(self):
        self.conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

    # DATABASE

    def flush_db(self):
        self.conn.flushdb()

    # WELLS
    @staticmethod
    def _get_well_id(wellname):
        return str(hashlib.md5(wellname.encode()).hexdigest())

    def create_well(self, wellname):
        self.update_well_info(wellname, {'name': wellname, 'datasets': [], })

    def list_wells(self):
        data = [json.loads(d) for d in self.conn.hgetall("wells").values()]
        return {d['name']: d for d in data}

    def get_well_info(self, wellname):
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))

    def update_well_info(self, wellname, info):
        try:
            wellid = self._get_well_id(wellname)
            with self.conn.lock(f'wells:{wellid}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                return self.conn.hset("wells", wellid, json.dumps(info))
        except redis.exceptions.LockError:
            logger.error("Couldn't acquire lock in wells. Please try again later...")
            raise

    def delete_well(self, wellname):
        # delete datasets
        datasets = self.get_datasets(wellname)
        for dataset in datasets:
            self.delete_dataset(wellname, dataset)
        self.conn.hdel('wells', self._get_well_id(wellname))

    def get_datasets(self, wellname):
        wellinfo = self.get_well_info(wellname)
        return wellinfo.get('datasets', [])

    # DATASETS
    @staticmethod
    def _get_dataset_id(wellname, datasetname):
        return f"{wellname}__{datasetname}"

    def create_dataset(self, wellname, datasetname):
        well_id = self._get_well_id(wellname)

        try:
            # append dataset to well
            with self.conn.lock(f'well:{well_id}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                wellinfo = self.get_well_info(wellname)
                if 'datasets' not in wellinfo:
                    wellinfo['datasets'] = []
                wellinfo['datasets'].append(datasetname)
                self.update_well_info(wellname, wellinfo)
                return self._get_dataset_id(wellname, datasetname)
        except redis.exceptions.LockError:
            logger.error("Couldn't acquire lock in wells. Please try again later...")
            raise

    def set_dataset_info(self, wellname, datasetname, info):
        self.conn.hset('datasets', self._get_dataset_id(wellname, datasetname), json.dumps(info))

    def get_dataset_info(self, wellname, datasetname):
        return json.loads(self.conn.hget('datasets', self._get_dataset_id(wellname, datasetname)))

    def delete_dataset(self, wellname, datasetname):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        self.conn.delete(dataset_id)
        self.conn.hdel('datasets', dataset_id)
        wellinfo = self.get_well_info(wellname)
        wellinfo['datasets'].remove(datasetname)
        self.update_well_info(wellname, wellinfo)

    def read_dataset(self, wellname, datasetname, logs=None, depth=None, depth__gt=None, depth__lt=None):
        dataset_id = self._get_dataset_id(wellname, datasetname)

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
            self.conn.hset(self._get_dataset_id(wellname, datasetname), k, json.dumps(v))

    # LOGS
    def add_log(self, wellname, datasetname, log_name):
        self.conn.hset(self._get_dataset_id(wellname, datasetname), log_name, '{}')

    def update_logs(self, wellname, datasetname, data):
        self.conn.hset(self._get_dataset_id(wellname, datasetname), mapping=data)

    def delete_log(self, wellname, datasetname, log_name):
        self.conn.hdel(self._get_dataset_id(wellname, datasetname), log_name)
