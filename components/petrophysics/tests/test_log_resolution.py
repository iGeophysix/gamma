import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineProgress
from components.petrophysics.curve_operations import LogResolutionNode
from settings import BASE_DIR
from tasks import async_get_basic_log_stats, async_read_las, async_log_resolution

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


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
        async_log_resolution(dataset_id=wd.id, log_id='GK')

        log = BasicLog(wd.id, "GK")
        resolution = log.meta['log_resolution']['value']

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
        async_log_resolution(dataset_id=wd.id, log_id='AZIM')

        log = BasicLog(wd.id, 'AZIM')
        resolution = log.meta.log_resolution['value']

        self.assertTrue(resolution is None)


class TestLogResolutionNode(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self.wellname = '616'
        self.w = Well(self.wellname, new=True)

        # loading data
        filename = '616_ULN_ResolutionTest.las'
        self.wd = WellDataset(self.w, filename, new=True)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=os.path.join(PATH_TO_TEST_DATA, filename))

    def test_run(self):
        LogResolutionNode().start()

        log = BasicLog(self.wd.id, "GK_D0400_D")
        resolution = log.meta['log_resolution']['value']

        self.assertAlmostEqual(0.51, resolution, delta=0.001)
