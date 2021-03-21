import json
import os
import string
import time
import unittest

from datetime import datetime, timedelta
from random import randint, random, choice
from tasks import async_normalize_log

import numpy as np

from components.database.RedisStorage import RedisStorage

from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.domain.Project import Project
from components.importexport import las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestProject(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA

        for f in os.listdir(self.path_to_test_data):
            if not f.endswith('.las'):
                continue
            wellname = f[:-4]
            dataset_name = 'one'
            well = Well(wellname, True)
            dataset = WellDataset(well, dataset_name, True)

            las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                             well=well,
                             well_dataset=dataset)

    def test_tree(self):
        p = Project('test')
        tree = p.tree()
        for f in os.listdir(self.path_to_test_data):
            wellname = f[:-4]
            dataset_name = 'one'
            self.assertIn(wellname, tree)
            self.assertIn(dataset_name, tree[wellname])
            self.assertIn("GR", tree[wellname][dataset_name])

    def test_export_to_csv(self):
        p = Project('test')
        df = p.tree_df()
        df.to_csv('test_tree_df.csv', index=False)

