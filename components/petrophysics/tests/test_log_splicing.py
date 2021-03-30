import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from tasks import async_get_basic_log_stats, async_read_las, async_log_resolution, async_splice_logs

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestLogSplicing(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self.wellname = '609'
        self.w = Well(self.wellname, new=True)

        # loading data
        filename = '609_ULN_LQC.las'
        self.wd = WellDataset(self.w, filename, new=True)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=os.path.join(PATH_TO_TEST_DATA, filename))

        # calculating basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

        # adding more metadata
        meta = {
            'GK_D2258_D': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '70_(2450_2600)'},
            'GK_D1910_D': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '60_(2620_2700)'},
            'GK_D1911_D_2': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '50_(2300_2700)'},
            'GK_D2395_D': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '40_(600_800)'},
            'GK_D2265_D': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '30_(400_2450)'},
            'GK_D1911_D': {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '20_(50_2700)'},
        }
        self.wd.append_log_meta(meta)

        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

    def test_log_splicing_works_correctly(self):
        async_splice_logs(wellname='609')
