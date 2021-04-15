import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.shale_volume import linear_method, larionov_older_rock_method, larionov_tertiary_rock_method
from tasks import async_get_basic_log_stats, async_read_las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestShaleVolume(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '616'
        self.w = Well(wellname, new=True)
        # loading data
        filename = '616_D0401_D.las'
        self.wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, 'HugeWell616_125D_1283V', filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

    def test_shale_volume_linear_works_correctly(self):
        gk = BasicLog(self.wd.id, "GK")
        q5 = 5.149  # np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = 8.53  # np.quantile(gk.non_null_values[:, 1], 0.95)
        vsh_gr = linear_method(gk, q5, q95)
        vsh_gr.dataset_id = self.wd.id
        vsh_gr.name = "VSH_GR"
        vsh_gr.save()

    def test_shale_volume_larionov_older_works_correctly(self):
        gk = BasicLog(self.wd.id, "GK")
        q5 = 5.149  # np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = 8.53  # np.quantile(gk.non_null_values[:, 1], 0.95)
        vsh_gr = larionov_older_rock_method(gk, q5, q95)
        vsh_gr.dataset_id = self.wd.id
        vsh_gr.name = "VSH_GR"
        vsh_gr.save()


    def test_shale_volume_larionov_tertiary_works_correctly(self):
        gk = BasicLog(self.wd.id, "GK")
        q5 = 5.149  # np.quantile(gk.non_null_values[:, 1], 0.05)
        q95 = 8.53  # np.quantile(gk.non_null_values[:, 1], 0.95)
        vsh_gr = larionov_tertiary_rock_method(gk, q5, q95)
        vsh_gr.dataset_id = self.wd.id
        vsh_gr.name = "VSH_GR"
        vsh_gr.save()
