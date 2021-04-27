import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog, MarkersLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.shale_volume import ShaleVolumeLinearMethod, ShaleVolumeLarionovOlderRock, ShaleVolumeLarionovTertiaryRock
from tasks import async_get_basic_log_stats, async_read_las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestShaleVolume(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '15-9-19'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'Volve_15-9-19_VSH.las'
        self.wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])
        gk = BasicLog(self.wd.id, "GR")
        gk.meta.family = 'Gamma Ray'
        gk.save()

    def test_shale_volume_linear_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLinearMethod()
        vsh_gr = module.run(gk, q5, q95, "VSH_GR")
        vsh_gr.dataset_id = self.wd.id

        true_vsh = BasicLog(self.wd.id, "VSH_GR_linear")
        diff = vsh_gr.values[:, 1] - true_vsh[:, 1]
        self.assertAlmostEqual(0.0, np.nanmin(diff, ), 4)
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 4)

    def test_shale_volume_larionov_older_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLarionovOlderRock()
        vsh_gr = module.run(gk, q5, q95, "VSH_GR")
        vsh_gr.dataset_id = self.wd.id
        vsh_gr.save()

        true_vsh = BasicLog(self.wd.id, "VSH_GR_LarOlder")
        diff = vsh_gr.values[:, 1] - true_vsh[:, 1]
        self.assertAlmostEqual(0.0, np.nanmin(diff, ), 4)
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 4)

    def test_shale_volume_larionov_tertiary_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLarionovTertiaryRock()
        vsh_gr = module.run(gk, q5, q95, "VSH_GR")
        vsh_gr.dataset_id = self.wd.id

        true_vsh = BasicLog(self.wd.id, "VSH_GR_LarTert")
        diff = abs(vsh_gr.values[:, 1] - true_vsh[:, 1])
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 3)

    def test_shale_volume_larionov_tertiary_validation_works(self):
        module = ShaleVolumeLarionovTertiaryRock()
        # check works with child type of logs
        other_log = MarkersLog(self.wd.id, "GR")
        module.run(other_log, 0, 10)

        gk = BasicLog(self.wd.id, "GR")
        q5, q95 = 0, 10

        # values won't pass validation
        with self.assertRaises(ValueError):
            module.run(gk, q95, q5)

        # log is not of class BasicLog
        with self.assertRaises(TypeError):
            module.run(gk.values, q5, q95)

        # log family is incorrect
        gk.meta.family = 'Compressional Slowness'
        with self.assertRaises(AssertionError):
            module.run(gk, q5, q95)
