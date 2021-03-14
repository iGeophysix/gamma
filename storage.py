import hashlib
import json
import logging
import sys

import redis

from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

logger = logging.getLogger("storage")

BLOCKING_TIMEOUT = 5


def md5(text):
    return str(hashlib.md5(text.encode()).hexdigest())


class RedisStorage:
    def __init__(self):
        self.conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)

    # DATABASE

    def flush_db(self):
        self.conn.flushdb()

    # WELLS

    @staticmethod
    def _get_well_id(wellname):
        return md5(wellname)

    def create_well(self, wellname):
        wellid = self._get_well_id(wellname)
        try:
            with self.conn.lock(f'wells:{wellid}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                self.conn.hset('wells', wellid, json.dumps({'name': wellname, 'datasets': [], 'meta': {}}))
        except redis.exceptions.LockError:
            logger.error("Couldn't acquire lock in wells. Please try again later...")
            raise

    def list_wells(self):
        data = [json.loads(d) for d in self.conn.hgetall("wells").values()]
        return {d['name']: d for d in data}

    def get_well_name(self, wellname):
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))['name']

    def get_well_datasets(self, wellname):
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))['datasets']

    def get_well_info(self, wellname):
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))['meta']

    def update_well_info(self, wellname, info):
        try:
            wellid = self._get_well_id(wellname)
            with self.conn.lock(f'wells:{wellid}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                current_info = json.loads(self.conn.hget('wells', wellid))
                current_info['meta'] = info
                self.conn.hset("wells", wellid, json.dumps(current_info))
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
        dataset_ids = self.get_well_datasets(wellname)
        return [json.loads(self.conn.hget('datasets', d))['name'] for d in dataset_ids]

    # DATASETS
    @staticmethod
    def _get_dataset_id(wellname, datasetname):
        return md5(f"{wellname}__{datasetname}")

    def create_dataset(self, wellname, datasetname):
        well_id = self._get_well_id(wellname)
        dataset_id = self._get_dataset_id(wellname, datasetname)
        dataset_info = {'name': datasetname, 'meta': {}}
        # append dataset to well
        with self.conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'wells:{well_id}')
                    pipe.watch(f'datasets:{dataset_id}')
                    wellinfo = json.loads(pipe.hget('wells', well_id))
                    if 'datasets' not in wellinfo:
                        wellinfo['datasets'] = []
                    wellinfo['datasets'].append(dataset_id)
                    pipe.multi()
                    pipe.hset("wells", well_id, json.dumps(wellinfo))
                    pipe.hset("datasets", dataset_id, json.dumps(dataset_info))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

        return dataset_id

    def get_dataset_name(self, dataset_id):
        return json.loads(self.conn.hget('datasets', dataset_id))['name']

    def get_dataset_logs(self, wellname, datasetname):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        return [l.decode() for l in self.conn.hkeys(f'{dataset_id}_meta')]

    def get_dataset_info(self, wellname=None, datasetname=None, dataset_id=None):
        if dataset_id is None:
            dataset_id = self._get_dataset_id(wellname, datasetname)
        if self.conn.hexists('datasets', dataset_id):
            return json.loads(self.conn.hget('datasets', dataset_id))
        else:
            raise FileNotFoundError(f"Dataset {datasetname} was not found in well {wellname}")

    def set_dataset_info(self, wellname, datasetname, info):
        dataset_id = self._get_dataset_id(wellname, datasetname)

        with self.conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'datasets:{dataset_id}')
                    current_info = self.get_dataset_info(wellname, datasetname)
                    pipe.multi()
                    current_info['meta'] = info
                    pipe.hset('datasets', dataset_id, json.dumps(current_info))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

    def delete_dataset(self, wellname, datasetname):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        well_id = self._get_well_id(wellname)
        with self.conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'datasets:{dataset_id}')
                    pipe.watch(f'wells:{well_id}')
                    pipe.multi()
                    pipe.delete(dataset_id)
                    pipe.delete(f"{dataset_id}_meta")
                    pipe.hdel('datasets', dataset_id)
                    wellinfo = json.loads(self.conn.hget('wells', well_id))
                    wellinfo['datasets'].remove(dataset_id)
                    pipe.hset('wells', well_id, json.dumps(wellinfo))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

    def read_dataset(self, wellname, datasetname, logs=None, depth=None, depth__gt=None, depth__lt=None):
        dataset_id = self._get_dataset_id(wellname, datasetname)

        def slice(data, top=None, bottom=None):
            top = top if top is not None else sys.float_info.min
            bottom = bottom if bottom is not None else sys.float_info.max
            return {float(k): v for k, v in data.items() if float(k) >= top and float(k) <= bottom}

        def beautify_depths(data):
            return {float(k): v for k, v in data.items()}

        if logs:
            out = {log: json.loads(self.conn.hget(dataset_id, log)) for log in logs}
        else:
            out = {k.decode('utf-8'): beautify_depths(json.loads(v)) for k, v in self.conn.hgetall(dataset_id).items()}

        # apply slicing
        if depth is None and depth__gt is None and depth__lt is None:
            return out
        elif depth__gt is not None or depth__lt is not None:
            return {l: slice(v, depth__gt, depth__lt) for l, v in out.items()}
        else:
            return {l: slice(v, depth, depth) for l, v in out.items()}

    # LOGS
    def add_log(self, wellname, datasetname, log_name):
        self.conn.hset(self._get_dataset_id(wellname, datasetname), log_name, '{}')

    def get_logs_meta(self, wellname, datasetname, logs=None):
        dataset_id = self._get_dataset_id(wellname, datasetname)

        if logs:
            out = {log: json.loads(self.conn.hget(f"{dataset_id}_meta", log)) for log in logs}
        else:
            out = {k.decode('utf-8'): json.loads(v) for k, v in self.conn.hgetall(f"{dataset_id}_meta").items()}

        return out

    def update_logs(self, wellname, datasetname, data=None, meta=None):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        if meta:
            self.conn.hset(f"{dataset_id}_meta", mapping={k: json.dumps(v) for k, v in meta.items()})
        if data:
            self.conn.hset(dataset_id, mapping={k: json.dumps(v) for k, v in data.items()})

    def delete_log(self, wellname, datasetname, log_name):
        self.conn.hdel(self._get_dataset_id(wellname, datasetname), log_name)

    def log_history(self, wellname, datasetname, log):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        log_meta = json.loads(self.conn.hget(f"{dataset_id}_meta", log))
        return log_meta.get('__history', [])

    def append_log_history(self, wellname, datasetname, log, event):
        dataset_id = self._get_dataset_id(wellname, datasetname)
        with self.conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f"{dataset_id}_meta:{log}")
                    meta = json.loads(pipe.hget(f"{dataset_id}_meta", log))
                    pipe.multi()
                    if '__history' not in meta.keys():
                        meta['__history'] = []
                    meta['__history'].append(event)
                    pipe.hset(f"{dataset_id}_meta", log, json.dumps(meta))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

