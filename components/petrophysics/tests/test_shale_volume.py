import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.shale_volume import ShaleVolumeLinearMethodNode, ShaleVolumeLarionovOlderRockNode, ShaleVolumeLarionovTertiaryRockNode
from settings import BASE_DIR
from tasks import async_get_basic_log_stats, async_read_las

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')

class TestShaleVolume(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '15-9-19'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'Volve_15-9-19_VSH.las'
        self.wd = WellDataset(self.w, 'LQC', new=True)
        self.output_wd = WellDataset(self.w, 'LQC', new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname='LQC', filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=['LQC', ])
        gk = BasicLog(self.wd.id, "GR")
        gk.meta.family = 'Gamma Ray'
        gk.save()

    def test_shale_volume_linear_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLinearMethodNode()
        module.run(q5, q95)

        vsh_gr = BasicLog(self.output_wd.id, "VSH_GR_LM")
        true_vsh = BasicLog(self.wd.id, "VSH_GR_linear")
        diff = vsh_gr.values[:, 1] - true_vsh[:, 1]
        self.assertAlmostEqual(0.0, np.nanmin(diff, ), 4)
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 4)

    def test_shale_volume_larionov_older_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLarionovOlderRockNode()
        module.run(q5, q95)

        vsh_gr = BasicLog(self.output_wd.id, "VSH_GR_LOR")

        true_vsh = BasicLog(self.wd.id, "VSH_GR_LarOlder")
        diff = vsh_gr.values[:, 1] - true_vsh[:, 1]
        self.assertAlmostEqual(0.0, np.nanmin(diff, ), 4)
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 4)

    def test_shale_volume_larionov_tertiary_works_correctly(self):
        gk = BasicLog(self.wd.id, "GR")
        q5 = np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = np.quantile(gk.non_null_values[:, 1], 0.95)
        module = ShaleVolumeLarionovTertiaryRockNode()
        module.run(q5, q95)

        vsh_gr = BasicLog(self.output_wd.id, "VSH_GR_LTR")
        true_vsh = BasicLog(self.wd.id, "VSH_GR_LarTert")
        diff = abs(vsh_gr.values[:, 1] - true_vsh[:, 1])
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 3)
