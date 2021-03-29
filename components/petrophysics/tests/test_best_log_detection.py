import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.best_log_detection import get_best_log
from tasks import async_get_basic_log_stats, async_read_las, async_log_resolution

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestBestLogDetection(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self.wellname = '616'
        self.w = Well(self.wellname, new=True)

        # loading data
        filename = '616_ULN_ResolutionTest.las'
        self.wd = WellDataset(self.w, filename, new=True)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=os.path.join(PATH_TO_TEST_DATA, filename))

        # calculating basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

        # adding more metadata
        meta = {log: {'log_family': 'Gamma Ray', 'Run_AutoCalculated': '56_(2650_2800)'} for log in self.wd.get_log_list()}
        self.wd.append_log_meta(meta)

        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

    def test_best_log_detection_works_correct(self):
        best_log, new_meta = get_best_log(dataset=self.wd, log_family='Gamma Ray', run_name='56_(2650_2800)')
        self.wd.append_log_meta(new_meta)

        self.assertEqual('GK_D0403_D', best_log, msg='Best log in this dataset is GK_D8107_D')
        log_meta = self.wd.get_log_meta()
        self.assertEqual(True, log_meta['GK_D0403_D']['BestLog_AutoCalculated'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals True')
        self.assertEqual(False, log_meta['GK_D1800_D']['BestLog_AutoCalculated'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals False')

        # delete the GK_D0403_D and check  GK_D8107_D_2 becomes the best
        self.wd.delete_log('GK_D0403_D')
        best_log, new_meta = get_best_log(dataset=self.wd, log_family='Gamma Ray', run_name='56_(2650_2800)')
        self.wd.append_log_meta(new_meta)
        self.assertEqual('GK_D8107_D_2', best_log, msg='Best log in this dataset is GK_D8107_D')
        log_meta = self.wd.get_log_meta()
        self.assertEqual(True, log_meta['GK_D8107_D_2']['BestLog_AutoCalculated'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals True')
        self.assertEqual(False, log_meta['GK_D1800_D']['BestLog_AutoCalculated'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals False')
