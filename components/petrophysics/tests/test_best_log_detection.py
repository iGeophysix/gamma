import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.best_log_detection import get_best_log, BestLogDetectionNode, score_log_tags, LOG_TAG_ASSESSMENT
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
        # deleting duplicated log
        self.wd.delete_log('GR_D4417_D')
        # calculating basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

        # adding more metadata
        for log_id in self.wd.get_log_list():
            log = BasicLog(self.wd.id, log_id)
            log.meta.update({'family': 'Gamma Ray', 'run': {'value': '56_(2650_2800)'}})
            log.save()

        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

    def test_best_log_detection_works_correct(self):
        best_log, new_meta = get_best_log(datasets=[self.wd,], family='Gamma Ray', run_name='56_(2650_2800)')

        for log_id, values in new_meta.items():
            l = BasicLog(self.wd.id, log_id)
            l.meta = values
            l.save()

        self.assertEqual('GK_D4417_D', best_log, msg='Best log in this dataset is GK_D4417_D')
        log1 = BasicLog(self.wd.id, 'GK_D4417_D')
        log2 = BasicLog(self.wd.id, 'GK_D1800_D')
        self.assertEqual(True, log1.meta.best_log_detection['is_best'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals True')
        self.assertEqual(False, log2.meta.best_log_detection['is_best'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals False')

    def test_best_log_detection_engine_node_works_correctly(self):
        bld = BestLogDetectionNode()
        bld.run()

        log1 = BasicLog(self.wd.id, 'GK_D4417_D')
        log2 = BasicLog(self.wd.id, 'GK_D1800_D')

        self.assertEqual(True, log1.meta.best_log_detection['is_best'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals True')
        self.assertEqual(False, log2.meta.best_log_detection['is_best'], msg='Record in metadata of log should be BestLog_AutoCalculated and equals False')

    def test_score_log_tags(self):
        right_answer = sum(LOG_TAG_ASSESSMENT.values())
        answer = 0
        for tag in LOG_TAG_ASSESSMENT:
            tags = []
            tags.append(tag + '&' + tag)    # an unknown but looks similar to a known tag
            mixed_case = ''.join(letter.upper() if n % 2 else letter.lower() for n, letter in enumerate(tag))
            tags.append(mixed_case)     # a known tag, mixed case
            answer += score_log_tags(tags)
        self.assertEqual(answer, right_answer)
