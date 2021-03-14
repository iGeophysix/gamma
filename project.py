import logging

import pandas as pd

from storage import RedisStorage

logging.basicConfig()
debug = logging.getLogger("gamma")
debug.setLevel(logging.INFO)


class Project:
    def __init__(self, name):
        self._name = name

    def list_wells(self):
        _s = RedisStorage()
        return _s.list_wells()

    def tree(self):
        _s = RedisStorage()
        tree = {}
        wells = _s.list_wells()
        for well, well_info in wells.items():
            tree.update({well: {}})
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = _s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = _s.get_logs_meta(well, dataset_name)
                tree[well].update({dataset_name: dataset_logs})
        return tree

    def tree_df(self):
        _s = RedisStorage()
        out = []
        wells = _s.list_wells()
        for well, well_info in wells.items():
            dataset_ids = well_info['datasets']
            for dataset_id in dataset_ids:
                dataset_info = _s.get_dataset_info(dataset_id=dataset_id)
                dataset_name = dataset_info['name']
                dataset_logs = _s.get_logs_meta(well, dataset_name)
                out.extend([[well, dataset_name, log, log_info] for log, log_info in dataset_logs.items()])
        return pd.DataFrame(out, columns=['well', 'dataset', 'log', 'meta'])


