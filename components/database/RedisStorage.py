import hashlib
import io
import json
import logging
import sys
from typing import Any

import numpy as np
import redis

from components.database.settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

logger = logging.getLogger("storage")

BLOCKING_TIMEOUT = 5


def md5(text):
    return str(hashlib.md5(text.encode()).hexdigest())


class RedisStorage:
    """
    Interface class to work with Redis as main storage
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD to be set via environmental variables
    """

    def __init__(self):
        self.conn = redis.Redis(host=REDIS_HOST,
                                port=REDIS_PORT,
                                db=REDIS_DB,
                                password=REDIS_PASSWORD)

    # DATABASE

    def flush_db(self):
        """Erase the whole database.
        Use with caution!"""
        self.conn.flushdb()

    # WELLS

    @staticmethod
    def _get_well_id(wellname: str) -> str:
        """
        Method to generate unique well id. Hope that md5 can do that :-)
        :param wellname: well name as string
        :return: unique well id for database
        """
        return md5(wellname)

    def create_well(self, wellname: str):
        """
        Creates a well in database with empty meta
        :param wellname: well name as string
        """
        wellid = self._get_well_id(wellname)
        self.conn.hset('wells', wellid, json.dumps({'name': wellname, 'datasets': [], 'meta': {}}))

    def list_wells(self) -> dict:
        """
        Lists all well meta-information in the storage
        :return: dict
            Example: {'well1': {'name': 'well1', 'datasets': [], 'meta': {}}, 'well2': {'name': 'well2', 'datasets': [], 'meta': {}}}
        """
        data = [json.loads(d) for d in self.conn.hgetall("wells").values()]
        return {d['name']: d for d in data}

    def get_well_datasets(self, wellname: str) -> list:
        """
        Returns list of dataset ids associated with the well
        :param wellname: well name as string
        :return: list of dataset ids
        """
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))['datasets']

    def get_well_info(self, wellname: str) -> dict:
        """
        Returns well meta info by well name
        :param wellname: well name as string
        :return: dict with all info in it
        """
        return json.loads(self.conn.hget('wells', self._get_well_id(wellname)))['meta']

    def set_well_info(self, wellname: str, info: dict) -> None:
        """
        Sets well meta info.
        :param wellname: well name as string
        :param info: dict with well meta info
        :exception LockError: Can raise an exception if cannot obtain lock on well in Redis
        """
        try:
            wellid = self._get_well_id(wellname)
            with self.conn.lock(f'wells:{wellid}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                current_info = json.loads(self.conn.hget('wells', wellid))
                current_info['meta'] = info
                self.conn.hset("wells", wellid, json.dumps(current_info))
        except redis.exceptions.LockError:
            logger.error("Couldn't acquire lock in wells. Please try again later...")
            raise

    def delete_well(self, wellname: str) -> None:
        """
        Deletes well and all its datasets
        :param wellname: well name
        """
        # delete datasets
        datasets = self.get_datasets(wellname)
        for dataset in datasets:
            self.delete_dataset(wellname, dataset)
        self.conn.hdel('wells', self._get_well_id(wellname))

    def get_datasets(self, wellname: str) -> list[str]:
        """
        Returns list of dataset names in the well
        :param wellname: well name as string
        :return: list of datasat namesß
        """
        dataset_ids = self.get_well_datasets(wellname)
        return [self.get_dataset_name(d) for d in dataset_ids]

    # DATASETS
    @staticmethod
    def _get_dataset_id(wellname: str, datasetname: str) -> str:
        """
        Returns a unique dataset id. Hope that MD5 can generate unique values forever :-)
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :return: string with dataset_id
        """
        return md5(f"{wellname}__{datasetname}")

    def create_dataset(self, wellname: str, datasetname: str) -> str:
        """
        Creates an empty dataset and registers it in the well
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :return: dataset_id as string
        """
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

    def get_dataset_name(self, dataset_id: str) -> str:
        """
        Get dataset name by dataset_id
        :param dataset_id: dataset_id as string
        :return: dataset name as string
        """
        return json.loads(self.conn.hget('datasets', dataset_id))['name']

    def get_dataset_logs(self, wellname: str, datasetname: str) -> list[str]:
        """
        Get list of logs in the dataset
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :return: list of logs available in the dataset
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)
        return [l.decode() for l in self.conn.hkeys(f'{dataset_id}_meta')]

    def get_dataset_info(self, wellname: str = None, datasetname: str = None, dataset_id: str = None) -> dict:
        """
        Get dataset info by wellname and datasetname or by dataset_id
        :param wellname: if dataset_id is None, then specify well name to find the dataset
        :param datasetname: if dataset_id is None, then specify dataset name to find the dataset
        :param dataset_id: if not None, not necessary to specify wellname and datasetname
        :return: dictinoary with all dataset meta information
        """
        if dataset_id is None:
            dataset_id = self._get_dataset_id(wellname, datasetname)
        if self.conn.hexists('datasets', dataset_id):
            return json.loads(self.conn.hget('datasets', dataset_id))
        else:
            raise FileNotFoundError(f"Dataset {datasetname} was not found in well {wellname}")

    def set_dataset_info(self, wellname: str, datasetname: str, info: dict) -> None:
        """
        Sets new dataset meta info
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param info: dict with new meta information

        """
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

    def delete_dataset(self, wellname: str, datasetname: str) -> None:
        """
        Deletes dataset and removes its record from the well entry
        :param wellname: well name as string
        :param datasetname: dataset name as string
        """
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

    # LOGS
    def add_log(self, wellname: str, datasetname: str, log_name: str) -> None:
        """
        Creates an empty log in dataset. Inserts records in dataset and dataset meta tables
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param log_name: log name as string
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)
        self.conn.hset(f"{dataset_id}_meta", log_name, '{}')
        self.conn.hset(dataset_id, log_name, '{}')

    def get_logs_data(self, wellname: str,
                      datasetname: str,
                      logs: list[str] = None,
                      depth: float = None,
                      depth__gt: float = None,
                      depth__lt: float = None): #-> dict[np.array]:
        """
        Returns dict with logs data. Depth references are signed - pay attention to depth reference sign
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param logs: if None, then all logs will be returned. Log names in list of strings (use ["GR",] if you only need one log)
        :param depth: if specific depth reference is required specify this parameter
        :param depth__gt: if specified then depth below is ignored and log will be sliced by depth > depth__gt
        :param depth__lt: if specified then depth above is ignored and log will be sliced by depth < depth__lt
        :return: dict with logs and values
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)

        def slice(data, top=None, bottom=None):
            top = top if top is not None else sys.float_info.min
            bottom = bottom if bottom is not None else sys.float_info.max

            data = data[data[:, 0] >= top]
            data = data[data[:, 0] <= bottom]
            return data

        if logs == None:
            logs = [l.decode() for l in self.conn.hkeys(dataset_id)]

        out = {log: np.load(io.BytesIO(self.conn.hget(dataset_id, log)), allow_pickle=True) for log in logs}

        # apply slicing
        if depth is None and depth__gt is None and depth__lt is None:
            return out
        elif depth__gt is not None or depth__lt is not None:
            return {l: slice(v, depth__gt, depth__lt) for l, v in out.items()}
        else:
            return {l: slice(v, depth, depth) for l, v in out.items()}

    def get_logs_meta(self, wellname: str, datasetname: str, logs=None) -> dict:
        """
        Returns dict with meta info of logs in the dataset.
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param logs: if None then returns all logs' meta. Log names in list of strings (use ["GR",] if you only need one log)
        :return: dict with each log info as dict
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)

        if logs == None:
            logs = [l.decode() for l in self.conn.hkeys(f"{dataset_id}_meta")]

        out = {log: json.loads(self.conn.hget(f"{dataset_id}_meta", log)) for log in logs}

        return out

    def append_log_meta(self, wellname: str, datasetname: str, logname: str, meta: dict) -> None:
        """
        Append meta information to a log
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param logname: log name as string
        :param meta: dict with new meta information (e.g. {"UWI":434232, "PWA":"GIGI",...})
        """

        dataset_id = self._get_dataset_id(wellname, datasetname)
        with self.conn.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'{dataset_id}_meta:{logname}')
                    current_info = json.loads(pipe.hget(f'{dataset_id}_meta', logname))
                    pipe.multi()
                    current_info.update(meta)
                    pipe.hset(f'{dataset_id}_meta', logname, json.dumps(current_info))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

    def update_logs(self, wellname: str, datasetname: str, data=None, meta=None) -> None:
        """
        Sets log data and meta information.
        It will add empty history key to log meta if __history key is not found in meta of the log
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param data: data as dict (e.g. {"GR": np.array((10.0,1), (20.0:2),..), "PS":...}
        :param meta: meta info as dict (e.g. {"GR": {"units":"gAPI", "code":"tt"}, "PS":...}
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)

        if meta:
            for log in meta.keys():
                meta[log]['__history'] = meta[log].get('__history', [])
            self.conn.hset(f"{dataset_id}_meta",
                           mapping={k: json.dumps(v) for k, v in meta.items()})
        if data:
            # convert np.array to bytes
            mapping = {}
            for k, v in data.items():
                non_null_values = v[~np.isnan(v[:, 1])]
                stream = io.BytesIO()
                # np.savez_compressed(stream, array=v)
                np.save(stream, non_null_values, allow_pickle=True)
                mapping[k] = stream.getvalue()  # bytes

            self.conn.hset(dataset_id, mapping=mapping)

    def delete_log(self, wellname: str, datasetname: str, log_name: str) -> None:
        """
        Deletes log from the storage
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param log_name: log name as string
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)
        self.conn.hdel(dataset_id, log_name)
        self.conn.hdel(f"{dataset_id}_meta", log_name)

    def log_history(self, wellname: str, datasetname: str, log: str) -> list[Any]:
        """
        Returns log history as list of events.
        If history key is not found returns empty list.
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param log: log name as string
        :return: list of events
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)
        log_meta = json.loads(self.conn.hget(f"{dataset_id}_meta", log))
        return log_meta.get('__history', [])

    def append_log_history(self, wellname: str, datasetname: str, log: str, event: Any) -> None:
        """
        Appends event to the end of the log history.
        :param wellname: well name as string
        :param datasetname: dataset name as string
        :param log: log name as string
        :param event: event must be serializable
        """
        dataset_id = self._get_dataset_id(wellname, datasetname)

        with self.conn.lock(f"{dataset_id}_meta:{log}", blocking_timeout=BLOCKING_TIMEOUT) as lock:
            try:
                meta = json.loads(self.conn.hget(f"{dataset_id}_meta", log))
                if '__history' not in meta.keys():
                    meta['__history'] = []
                meta['__history'].append(event)
                self.conn.hset(f"{dataset_id}_meta", log, json.dumps(meta))

            except redis.exceptions.LockError:
                logger.error("Couldn't acquire lock in wells. Please try again later...")
                raise
