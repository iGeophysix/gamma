import os
from unittest import TestCase

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.domain.Log import BasicLog
from tasks import async_read_las
from components.petrophysics.curve_operations import interpolate_log_by_depth, BasicStatisticsNode
from settings import BASE_DIR

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


class TestBasicOperations(TestCase):
    def setUp(self) -> None:
        """
        Nothing to set up
        """
        pass

    def test_interpolate_log_by_depth(self):
        orig_md = np.linspace(1, 199, 100)
        orig_values = np.linspace(1, 199, 100)
        new_step = 1

        input_log = np.array(tuple(zip(orig_md, orig_values)))
        result = interpolate_log_by_depth(log_data=input_log, depth_step=new_step)
        self.assertListEqual(result[:, 1].tolist(), result[:, 0].tolist())

        # check cropping data in outside of "not nan data" interval works fine
        new_orig_md = np.concatenate((np.linspace(-51, -1, 26), orig_md, np.linspace(201, 599, 200)))
        new_orig_values = np.concatenate((np.full(26, np.nan), orig_values, np.full(200, np.nan)))
        input_log = np.vstack((new_orig_md, new_orig_values)).T
        new_result = interpolate_log_by_depth(log_data=input_log, depth_step=new_step)
        self.assertListEqual(result[:, 1].tolist(), new_result[:, 1].tolist())

        # check nan values inside remain
        new_orig_md = np.concatenate((np.linspace(-51, -1, 26), orig_md, np.linspace(201, 599, 200)))
        new_orig_values = np.concatenate((np.full(26, np.nan), orig_values, np.full(50, np.nan), np.full(50, 5), np.full(100, np.nan)))
        input_log = np.vstack((new_orig_md, new_orig_values)).T
        new_result = interpolate_log_by_depth(log_data=input_log, depth_step=new_step)
        self.assertTrue(np.isnan(new_result[199:300, 1]).all())


class TestBasicStatisticsNode(TestCase):
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
        true_value = {'avg_step': 0.1000001368263472,
                      'const_step': True,
                      'depth_span': 110.10011099999974,
                      'gmean': 6.193844100405169,
                      'max_depth': 2760.800116,
                      'max_value': 10.17982,
                      'mean': 6.299877084392014,
                      'min_depth': 2650.700005,
                      'min_value': 3.69,
                      'stdev': 1.155632528972132
                      }

        node = BasicStatisticsNode()
        node.start()

        l = BasicLog(self.wd.id, "GK_D8106_D")
        self.assertEqual(true_value, l.meta.basic_statistics)
