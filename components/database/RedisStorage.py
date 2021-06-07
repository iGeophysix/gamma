import hashlib
import io
import json
import logging
import sys
from collections import defaultdict
from typing import Any

import h5py
import redis

from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD, DEFAULT_MARKERS_NAME

logger = logging.getLogger("storage")
WELL_META_FIELDS_INDEX = 'well_meta_fields_index'
DATASET_META_FIELDS_INDEX = 'dataset_meta_fields_index'
LOG_META_FIELDS_INDEX = 'log_meta_fields_index'

BLOCKING_TIMEOUT = 5


def md5(text):
    return str(hashlib.md5(text.encode()).hexdigest())


class RedisStorage:
    """
    Interface class to work with Redis as main storage
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD to be set via environmental variables
    """

    _conn = redis.Redis(host=REDIS_HOST,
                        port=REDIS_PORT,
                        db=REDIS_DB,
                        password=REDIS_PASSWORD)

    def __init__(self):
        pass

    def connection(self):
        return RedisStorage._conn

    def __del__(self):
        self._conn.close()

    # DATABASE

    def flush_db(self):
        """Erase the whole database.
        Use with caution!"""
        # self.connection().flushdb()
        for wellname in self.list_wells():
            self.delete_well(wellname)
        for markerset in self.list_markersets():
            self.delete_markerset_by_name(markerset)

    # PROJECT
    def get_project_meta(self) -> dict:
        '''
        Get current project meta
        :return:
        '''
        project_meta = self.connection().get('project')
        if project_meta is not None:
            return json.loads(project_meta)
        else:
            return {}

    def set_project_meta(self, meta: dict):
        '''
        Set new project meta
        :param meta:
        :return:
        '''
        self.connection().set('project', json.dumps(meta))

    # COMMON META (FamilyProperties, Units,...)


    def table_exists(self, table_name) -> bool:
        """
        Checks if the table exists in the db.
        """
        return self.connection().exists(table_name) > 0

    def table_keys(self, table_name) -> list:
        """
        Lists all keys in the table
        :param table_name: Name of a hash-table
        :return: list
        """
        return [key.decode() for key in self.connection().hkeys(table_name)]

    def table_key_exists(self, table_name, key):
        return self.connection().hexists(table_name, key)

    def table_key_get(self, table_name, key):
        """
        General get method for hash-tables
        :param table_name:
        :param key:
        :return:
        """
        if self.table_key_exists(table_name, key):
            return json.loads(self.connection().hget(table_name, key))
        else:
            raise KeyError(f"Key {key} wasn't found in table {table_name}")

    def table_key_set(self, table_name, key=None, data=None, mapping=None):
        """
        General set method for hash-tables
        :param table_name:
        :param key:
        :param data:
        :return:
        """
        if mapping:
            encoded_data = {key: json.dumps(val) for key, val in mapping.items()}
            self.connection().hset(table_name, mapping=encoded_data)
        else:
            self.connection().hset(table_name, key, json.dumps(data))

    def table_key_delete(self, table_name, key):
        """
        General delete method for hash-tables
        :param table_name:
        :param key:
        :return:
        """
        if self.table_key_exists(table_name, key):
            self.connection().hdel(table_name, key)

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
        self.connection().hset('wells', wellid, json.dumps({'name': wellname, 'datasets': [], 'meta': {}}))

    def check_well_exists(self, wellname: str) -> bool:
        """
        Simple function to check if the well exists in the db
        :param wellname:
        :return:
        """
        return self.connection().hexists('wells', self._get_well_id(wellname))

    def list_wells(self) -> dict:
        """
        Lists all well meta-information in the storage
        :return: dict
            Example: {'well1': {'name': 'well1', 'datasets': [], 'meta': {}}, 'well2': {'name': 'well2', 'datasets': [], 'meta': {}}}
        """
        data = [json.loads(d) for d in self.connection().hgetall("wells").values()]
        return {d['name']: d for d in data}

    def get_well_datasets(self, wellname: str) -> list:
        """
        Returns list of dataset ids associated with the well
        :param wellname: well name as string
        :return: list of dataset ids
        """
        return json.loads(self.connection().hget('wells', self._get_well_id(wellname)))['datasets']

    def get_well_info(self, wellname: str) -> dict:
        """
        Returns well meta info by well name
        :param wellname: well name as string
        :return: dict with all info in it
        """
        return json.loads(self.connection().hget('wells', self._get_well_id(wellname)))['meta']

    def set_well_info(self, wellname: str, info: dict) -> None:
        """
        Sets well meta info.
        :param wellname: well name as string
        :param info: dict with well meta info
        :exception LockError: Can raise an exception if cannot obtain lock on well in Redis
        """
        try:
            wellid = self._get_well_id(wellname)
            with self.connection().lock(f'wells:{wellid}', blocking_timeout=BLOCKING_TIMEOUT) as lock:
                current_info = json.loads(self.connection().hget('wells', wellid))
                current_info['meta'] = info
                self.connection().hset("wells", wellid, json.dumps(current_info))
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
        self.connection().hdel('wells', self._get_well_id(wellname))

    def get_datasets(self, wellname: str) -> list[str]:
        """
        Returns list of dataset names in the well
        :param wellname: well name as string
        :return: list of datasat names
        """
        dataset_ids = self.get_well_datasets(wellname)
        return [self.get_dataset_name(d) for d in dataset_ids]

    def get_well_name_by_id(self, well_id: str) -> str:
        """
        Returns well meta info by well name
        :param well_id: well id as string
        :return: dict with all info in it
        """
        return json.loads(self.connection().hget('wells', well_id))['name']

    # MARKERS SETS

    def list_markersets(self):
        """
        Get list of MarkersSets available in the project
        :return:
        """
        keys = [name.decode() for name in self.connection().hkeys("markersets")]
        return keys

    def check_markerset_exists(self, name: str) -> bool:
        """
        Check if MarkerSet name is exists in db
        :param name:
        :return:
        """
        return self.connection().hexists("markersets", name)

    def get_markerset_by_name(self, name: str) -> dict:
        """
        Get marker set by name
        :param name:
        :return: MarkerSet
        """
        raw_data = self.connection().hget('markersets', name)
        if raw_data is None:
            raise KeyError(f'MarkerSet with name {name} was not found')

        marker_set = json.loads(raw_data)
        return marker_set

    def set_markerset_by_name(self, markerset: dict) -> None:
        """
        Update markerset in db
        :param markerset: MarkerSet
        :return:
        """
        name = markerset['name']
        self.connection().hset('markersets', name, json.dumps(markerset))

    def delete_markerset_by_name(self, name: str):
        """
        Delete makerset from project
        :param name:
        :return:
        """

        for well in self.markerset_well_ids(name):
            wellname = self.get_well_name_by_id(well)
            ds_id = self._get_dataset_id(wellname, DEFAULT_MARKERS_NAME)
            self.delete_log(ds_id, name)
        self.connection().hdel('markersets', name)

    def markerset_well_ids(self, name):
        """
        Get ids of wells having this markerset
        :param name: MarkerSet name
        :return:
        """
        well_ids = []
        for well_id in self.connection().hkeys('wells'):
            well_name = self.get_well_name_by_id(well_id)
            ms_id = self._get_dataset_id(well_name, DEFAULT_MARKERS_NAME)
            if self.connection().hexists(ms_id, name):
                well_ids.append(well_id.decode())
        return well_ids

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
        dataset_info = {'name': datasetname, 'well_name': wellname, 'meta': {}}
        # append dataset to well
        with self.connection().pipeline() as pipe:
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

    def check_dataset_exists(self, dataset_id: str) -> bool:
        """
        Simple function to check if the dataset exists in the db
        :param dataset_id:
        :return:
        """
        return self.connection().hexists('datasets', dataset_id)

    def get_dataset_name(self, dataset_id: str) -> str:
        """
        Get dataset name by dataset_id
        :param dataset_id: dataset_id as string
        :return: dataset name as string
        """
        return json.loads(self.connection().hget('datasets', dataset_id))['name']

    def get_dataset_logs(self, dataset_id: str) -> list[str]:
        """
        Get list of logs in the dataset
        :param dataset_id: dataset log_id as string
        :return: list of logs available in the dataset
        """
        return [l.decode() for l in self.connection().hkeys(f'{dataset_id}_meta')]

    def get_dataset_info(self, dataset_id: str = None) -> dict:
        """
        Get dataset info by dataset_id
        :param dataset_id: dataset id as string
        :return: dictionary with all dataset meta information
        """
        if self.connection().hexists('datasets', dataset_id):
            return json.loads(self.connection().hget('datasets', dataset_id))
        else:
            raise FileNotFoundError(f"Dataset {dataset_id} was not found")

    def set_dataset_info(self, dataset_id: str, info: dict) -> None:
        """
        Sets new dataset meta info
        :param dataset_id: dataset id as string
        :param info: dict with new meta information

        """

        with self.connection().pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'datasets:{dataset_id}')
                    current_info = self.get_dataset_info(dataset_id=dataset_id)
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
        with self.connection().pipeline() as pipe:
            while True:
                try:
                    pipe.watch(f'datasets:{dataset_id}')
                    pipe.watch(f'wells:{well_id}')
                    pipe.multi()
                    pipe.delete(dataset_id)
                    pipe.delete(f"{dataset_id}_meta")
                    pipe.hdel('datasets', dataset_id)
                    wellinfo = json.loads(self.connection().hget('wells', well_id))
                    wellinfo['datasets'].remove(dataset_id)
                    pipe.hset('wells', well_id, json.dumps(wellinfo))
                    pipe.execute()
                    break
                except redis.exceptions.WatchError:
                    continue

    # LOGS
    def add_log(self, dataset_id: str, log_name: str) -> None:
        """
        Creates an empty log in dataset. Inserts records in dataset and dataset meta tables
        :param dataset_id: dataset id as string
        :param log_name: log name as string
        """
        self.connection().hset(f"{dataset_id}_meta", log_name, '{}')
        self.connection().hset(dataset_id, log_name, '{}')

    def check_log_exists(self, dataset_id: str, log_name: str) -> bool:
        return self.connection().hexists(f'{dataset_id}_data', log_name) or self.connection().hexists(f'{dataset_id}_meta', log_name)

    def get_log_data(self, dataset_id: str,
                     logname: str,
                     depth: float = None,
                     depth__gt: float = None,
                     depth__lt: float = None):  # -> dict[np.array]:
        """
        Returns dict with one log data. Depth references are signed - pay attention to depth reference sign
        :param dataset_id: dataset id as string
        :param logname: log name as string (e.g. "GR")
        :param depth: if specific depth reference is required specify this parameter
        :param depth__gt: if specified then depth below is ignored and log will be sliced by depth > depth__gt
        :param depth__lt: if specified then depth above is ignored and log will be sliced by depth < depth__lt
        :return: dict with log_names and values
        """
        if not self.check_log_exists(dataset_id, logname):
            raise KeyError(f"Log {logname} wasn't found in dataset {dataset_id}")

        def slice(data, top=None, bottom=None):
            top = top if top is not None else sys.float_info.min
            bottom = bottom if bottom is not None else sys.float_info.max

            data = data[data[:, 0] >= top]
            data = data[data[:, 0] <= bottom]
            return data

        f = io.BytesIO(self.connection().hget(dataset_id, logname))
        with h5py.File(f, 'r') as hf:
            out = hf['values'][:]
            if out.dtype.char == 'S':
                out = out.astype('U')

        # apply slicing
        if depth is None and depth__gt is None and depth__lt is None:
            return out
        elif depth__gt is not None or depth__lt is not None:
            return slice(out, depth__gt, depth__lt)
        else:
            return slice(out, depth, depth)

    def get_logs_data(self, dataset_id: str,
                      logs: list[str] = None,
                      depth: float = None,
                      depth__gt: float = None,
                      depth__lt: float = None):  # -> dict[np.array]:
        """
        Returns dict with logs data. Depth references are signed - pay attention to depth reference sign
        :param dataset_id: dataset id as string
        :param logs: if None, then all logs will be returned. Log names in list of strings (use ["GR",] if you only need one log)
        :param depth: if specific depth reference is required specify this parameter
        :param depth__gt: if specified then depth below is ignored and log will be sliced by depth > depth__gt
        :param depth__lt: if specified then depth above is ignored and log will be sliced by depth < depth__lt
        :return: dict with logs and values
        """

        if logs == None:
            logs = [l.decode() for l in self.connection().hkeys(dataset_id)]

        out = {logname: self.get_log_data(dataset_id, logname, depth, depth__gt, depth__lt) for logname in logs}

        return out

    def get_log_meta(self, dataset_id: str, logname: str = None) -> dict:
        """
        Returns dict with meta info of one log in the dataset.
        :param dataset_id: dataset id as string
        :param logname: log name as strings (eg "GR" )
        :return: dict with log info as dict
        """
        return json.loads(self.connection().hget(f"{dataset_id}_meta", logname))

    def get_logs_meta(self, dataset_id: str, logs=None) -> dict:
        """
        Returns dict with meta info of logs in the dataset.
        :param dataset_id: dataset id as string
        :param logs: if None then returns all logs' meta. Log names in list of strings (use ["GR",] if you only need one log)
        :return: dict with each log info as dict
        """

        if logs == None:
            logs = [l.decode() for l in self.connection().hkeys(f"{dataset_id}_meta")]

        out = {log: self.get_log_meta(dataset_id, log) for log in logs}

        return out

    def append_log_meta(self, dataset_id: str, logname: str, meta: dict) -> None:
        """
        Append meta information to a log
        :param dataset_id: dataset id as string
        :param logname: log name as string
        :param meta: dict with new meta information (e.g. {"UWI":434232, "PWA":"GIGI",...})
        """
        with self.connection().pipeline() as pipe:
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

    def update_logs(self, dataset_id: str, data=None, meta=None) -> None:
        """
        Sets log data and meta information.
        It will add empty history key to log meta if history key is not found in meta of the log
        :param dataset_id: dataset id as string
        :param data: data as dict (e.g. {"GR": np.array((10.0,1), (20.0:2),..), "PS":...}
        :param meta: meta info as dict (e.g. {"GR": {"units":"gAPI", "code":"tt"}, "PS":...}
        """

        if meta:
            self.connection().hset(f"{dataset_id}_meta", mapping={k: json.dumps(v) for k, v in meta.items()})
        if data:
            # convert np.array to bytes
            mapping = {}
            for k, v in data.items():
                stream = io.BytesIO()
                with h5py.File(stream, 'w') as hf:
                    if v.dtype.char == 'U':
                        v = v.astype('S')
                    hf.create_dataset('values', data=v, )
                mapping[k] = stream.getvalue()  # bytes

            self.connection().hset(dataset_id, mapping=mapping)

    def delete_log(self, dataset_id: str, log_name: str) -> None:
        """
        Deletes log from the storage
        :param dataset_id: dataset id as string
        :param log_name: log name as string
        """
        self.connection().hdel(dataset_id, log_name)
        self.connection().hdel(f"{dataset_id}_meta", log_name)

    def log_history(self, dataset_id: str, log: str) -> list[Any]:
        """
        Returns log history as list of events.
        If history key is not found returns empty list.
        :param dataset_id: dataset id as string
        :param log: log name as string
        :return: list of events
        """
        log_meta = json.loads(self.connection().hget(f"{dataset_id}_meta", log))
        return log_meta.get('history', [])

    def append_log_history(self, dataset_id: str, log: str, event: Any) -> None:
        """
        Appends event to the end of the log history.
        :param dataset_id: dataset id as string
        :param log: log name as string
        :param event: event must be serializable
        """

        with self.connection().lock(f"{dataset_id}_meta:{log}", blocking_timeout=BLOCKING_TIMEOUT) as lock:
            try:
                meta = json.loads(self.connection().hget(f"{dataset_id}_meta", log))
                if 'history' not in meta.keys():
                    meta['history'] = []
                meta['history'].append(event)
                self.connection().hset(f"{dataset_id}_meta", log, json.dumps(meta))

            except redis.exceptions.LockError:
                logger.error("Couldn't acquire lock in wells. Please try again later...")
                raise


def flatten_keys(d, parent_key='', sep='.') -> list:
    """
    get flat list of nested dicts keys
    :param d: initial dictionary
    :param parent_key: key in initial dictionary to start from
    :param sep: serparator
    :return: list of flatten keys
    """
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_keys(v, new_key, sep=sep))
        else:
            items.append(new_key)
    return items


def build_log_meta_fields_index():
    '''
    This task builds index of meta fields available in logs
    :return:
    '''
    meta_field_index = defaultdict(list)

    # get list of datasets
    # then take each log meta and flatten it
    # then add it to meta field index
    s = RedisStorage()
    ds_ids = s.table_keys('datasets')
    for ds_id in ds_ids:
        ds_id_meta = f'{ds_id}_meta'
        for log_id in s.table_keys(ds_id_meta):
            meta = s.table_key_get(ds_id_meta, log_id)
            flat_meta = flatten_keys(meta)
            for meta_key in flat_meta:
                meta_field_index[meta_key].append((ds_id, log_id), )

    # store results
    # delete unused fields
    keys_to_delete = [key for key in s.table_keys(LOG_META_FIELDS_INDEX) if key not in meta_field_index.keys()]
    for key in keys_to_delete:
        s.table_key_delete(LOG_META_FIELDS_INDEX, key)

    # insert index
    if meta_field_index:
        s.table_key_set(LOG_META_FIELDS_INDEX, mapping=meta_field_index)

    return {'updated_keys': len(meta_field_index), 'deleted': len(keys_to_delete)}


def build_dataset_meta_fields_index():
    '''
    This task builds index of meta fields available in datasets
    :return:
    '''
    meta_field_index = defaultdict(list)

    # get list of datasets
    # then take each log meta and flatten it
    # then add it to meta field index
    s = RedisStorage()
    ds_ids = s.table_keys('datasets')
    for ds_id in ds_ids:
        meta = s.table_key_get('datasets', ds_id)
        flat_meta = flatten_keys(meta)
        for meta_key in flat_meta:
            meta_field_index[meta_key].append((ds_id), )

    # store results
    # delete unused fields
    keys_to_delete = [key for key in s.table_keys(DATASET_META_FIELDS_INDEX) if key not in meta_field_index.keys()]
    for key in keys_to_delete:
        s.table_key_delete(DATASET_META_FIELDS_INDEX, key)

    # insert index
    if meta_field_index:
        s.table_key_set(DATASET_META_FIELDS_INDEX, mapping=meta_field_index)

    return {'updated_keys': len(meta_field_index), 'deleted': len(keys_to_delete)}


def build_well_meta_fields_index():
    '''
    This task builds index of meta fields available in wells
    :return:
    '''
    meta_field_index = defaultdict(list)

    # get list of datasets
    # then take each log meta and flatten it
    # then add it to meta field index
    s = RedisStorage()
    well_ids = s.table_keys('wells')
    for well_id in well_ids:
        meta = s.table_key_get('wells', well_id)
        flat_meta = flatten_keys(meta)
        for meta_key in flat_meta:
            meta_field_index[meta_key].append((well_id), )

    # store results
    # delete unused fields
    keys_to_delete = [key for key in s.table_keys(WELL_META_FIELDS_INDEX) if key not in meta_field_index.keys()]
    for key in keys_to_delete:
        s.table_key_delete(WELL_META_FIELDS_INDEX, key)

    # insert index
    if meta_field_index:
        s.table_key_set(WELL_META_FIELDS_INDEX, mapping=meta_field_index)

    return {'updated_keys': len(meta_field_index), 'deleted': len(keys_to_delete)}
