import os
import unittest

import numpy as np

from components.petrophysics.curve_operations import interpolate_log_by_depth
from settings import BASE_DIR

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


class TestBasicOperations(unittest.TestCase):
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
