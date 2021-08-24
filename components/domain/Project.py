import datetime

import pandas as pd

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset


class Project:
    """
    Interface to storage to get project data in a convenient way
    """

    def __init__(self):
        pass

    @staticmethod
    def list_wells() -> dict:
        """
        Return dict of all well in the project with its info
        """
        _s = RedisStorage()
        return _s.list_wells()

    @staticmethod
    def tree() -> dict:
        """
        Returns all available object in a form of tree
        """
        _s = RedisStorage()
        tree = {}
        wells = _s.list_wells()
        for well, well_info in wells.items():
            tree.update({well: {}})
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = _s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = _s.get_logs_meta(dataset_id)
                tree[well].update({dataset_name: dataset_logs})
        return tree

    @staticmethod
    def tree_oop() -> dict:
        """
        Returns all available object in a form of tree with logs as objects
        {Well: {WellDataset: [BasicLog]}}
        """
        _s = RedisStorage()
        tree = {}
        wells = _s.list_wells()
        for well, _ in wells.items():
            w = Well(well)
            tree.update({w: {}})
            for dataset_name in w.datasets:
                ds = WellDataset(w, dataset_name)
                logs = [BasicLog(ds.id, log_name) for log_name in ds.log_list]
                tree[w].update({ds: logs})
        return tree

    @property
    def meta(self):
        _s = RedisStorage()
        return _s.get_project_meta()

    @meta.setter
    def meta(self, new_meta):
        _s = RedisStorage()
        _s.set_project_meta(meta=new_meta)

    def update_meta(self, new_meta):
        current_meta = self.meta
        self.meta = current_meta | new_meta

    @staticmethod
    def tree_df() -> pd.DataFrame:
        """
        Returns list of all available object in form of a pandas.DataFrame
        :return:
        """
        _s = RedisStorage()
        out = []
        wells = _s.list_wells()
        for well, well_info in wells.items():
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = _s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = _s.get_logs_meta(dataset_id)

                out.extend([{'well': well, 'dataset': dataset_name, 'log': log, **log_info} for log, log_info in dataset_logs.items()])
        return pd.DataFrame(out)

    @property
    def last_update(self) -> datetime.datetime:
        '''
        Get datetime whe project was last updated
        :return:
        '''
        _s = RedisStorage()
        prj_last_update = _s.get_project_meta().get('last_update', None)
        if prj_last_update is not None:
            return datetime.datetime.fromisoformat(prj_last_update)
        else:
            return None

    def set_updated(self):
        '''
        Set new last update datetime
        :return:
        '''
        _s = RedisStorage()
        current_meta = _s.get_project_meta() | {'last_update': datetime.datetime.now().isoformat()}
        _s.set_project_meta(current_meta)

    @property
    def markersets(self):
        """
        Get list of all markersets in the project
        :return:
        """
        _s = RedisStorage()
        return _s.list_markersets()
