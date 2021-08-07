import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.saturation import SaturationArchieNode
from settings import BASE_DIR
from tasks import async_read_las

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'saturation')


class TestSaturationArchieNode(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = 'Well1'
        self.w = Well(wellname, new=True)
        # loading data
        self.wd = WellDataset(self.w, 'LQC', new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, 'Well1_LQC.las')
        async_read_las(wellname=self.w.name, datasetname='LQC', filename=test_data)

        log = BasicLog(self.wd.id, 'PHIT_DUK')
        log.meta.family = 'Total Porosity'
        log.save()

        log = BasicLog(self.wd.id, 'RT_DUK')
        log.meta.family = 'Formation Resistivity'
        log.save()

    def test_saturation_archie_works_correctly(self):
        node = SaturationArchieNode()
        node.run(async_job=True)

        # compare with true results
        true_sw_ar = BasicLog(self.wd.id, 'SW_AR_TRUE')
        sw_ar = BasicLog(self.wd.id, 'SW_AR')
        true_sw_ar_vals = true_sw_ar.interpolate(sw_ar[:, 0])
        max_misfit = np.nanmax(abs(sw_ar[:, 1] - true_sw_ar_vals[:, 1]))
        self.assertLessEqual(max_misfit, 0.0001, 'Calculated Water Saturation is too different from true value')

        # true_sw_ar_uncl = BasicLog(self.wd.id, 'SW_AR_UNCL_TRUE')
        # sw_ar_uncl = BasicLog(self.wd.id, 'SW_AR_UNCL')
        # true_sw_ar_uncl_vals = true_sw_ar_uncl.interpolate(sw_ar[:, 0])
        # max_misfit = np.nanmax(abs(np.clip(sw_ar_uncl[:, 1], np.nanmin(true_sw_ar_uncl_vals[:, 1]), np.nanmax(true_sw_ar_uncl_vals[:, 1])) - true_sw_ar_uncl_vals[:, 1]))
        # self.assertLessEqual(max_misfit, 0.0001, 'Calculated Water Saturation is too different from true value')
