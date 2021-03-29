import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from tasks import async_get_basic_log_stats, async_read_las, async_log_resolution

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestLogResolution(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '616'
        self.w = Well(wellname, new=True)

    def test_log_resolution_correct(self):
        # loading data
        filename = '616_D0401_D.las'
        wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, 'HugeWell616_125D_1283V', filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])
        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

        meta = wd.get_log_meta(['GK', ])['GK']
        resolution = meta['LogResolution_AutoCalculated']

        self.assertAlmostEqual(0.5135, resolution, delta=0.001)

    def test_log_resolution_on_too_sparse_dataset(self):
        # loading data
        filename = '616_3.las'
        wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, 'HugeWell616_125D_1283V', filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])
        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

        meta = wd.get_log_meta(['AZIM', ])['AZIM']
        resolution = meta['LogResolution_AutoCalculated']

        self.assertTrue(np.isnan(resolution))
