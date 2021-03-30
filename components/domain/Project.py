import pandas as pd

from components.database.RedisStorage import RedisStorage


class Project:
    """
    Interface to storage to get project data in a convenient way
    """

    def __init__(self, name):
        self._s = RedisStorage()
        self._name = name

    def list_wells(self) -> dict:
        """
        Return dict of all well in the project with its info
        """
        _s = RedisStorage()
        return _s.list_wells()

    def tree(self) -> dict:
        """
        Returns all available object in a form of tree
        """
        _s = RedisStorage()
        tree = {}
        wells = self._s.list_wells()
        for well, well_info in wells.items():
            tree.update({well: {}})
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = self._s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = self._s.get_logs_meta(well, dataset_name)
                tree[well].update({dataset_name: dataset_logs})
        return tree

    def tree_df(self) -> pd.DataFrame:
        """
        Returns list of all available object in form of a pandas.DataFrame
        :return:
        """
        _s = RedisStorage()
        out = []
        wells = self._s.list_wells()
        for well, well_info in wells.items():
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = self._s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = self._s.get_logs_meta(well, dataset_name)

                out.extend([{'well': well, 'dataset': dataset_name, 'log': log, **log_info} for log, log_info in dataset_logs.items()])
        return pd.DataFrame(out)
