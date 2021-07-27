import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine import EngineProgress
from components.petrophysics.best_log_detection import (get_best_log_for_run_and_family,
                                                        BestLogDetectionNode,
                                                        score_log_tags,
                                                        best_rt)
from components.petrophysics.data.src.best_log_tags_assessment import read_best_log_tags_assessment
from settings import BASE_DIR
from tasks import (async_read_las,
                   async_log_resolution)

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


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

        # adding more metadata
        for log_id in self.wd.get_log_list():
            if log_id == 'MD':
                continue  # skip depth log
            log = BasicLog(self.wd.id, log_id)
            log.meta.update({'family': 'Gamma Ray', 'run': {'value': '56_(2650_2800)', 'top': 2650, 'bottom': 2800}})
            log.save()
            async_log_resolution(dataset_id=self.wd.id, log_id=log_id)

        self.wd.delete_log('GK_D4417_D')


    def test_best_log_detection_works_correct(self):
        best_log, new_meta = get_best_log_for_run_and_family(datasets=[self.wd, ],
                                                             family='Gamma Ray',
                                                             run_name='56_(2650_2800)')

        for log_id, values in new_meta.items():
            if log_id == 'MD':
                continue  # skip depth log
            l = BasicLog(self.wd.id, log_id)
            l.meta = values
            l.save()

        self.assertEqual('GK_D4417_D', best_log, msg='Best log in this dataset is GK_D4417_D')
        log1 = BasicLog(self.wd.id, 'GK_D4417_D')
        log2 = BasicLog(self.wd.id, 'GK_D1800_D')
        self.assertEqual(True, log1.meta.best_log_detection['is_best'], msg='GK_D4412_D is the best log')
        self.assertEqual(False, log2.meta.best_log_detection['is_best'], msg='GK_D1800_D is not the best log')

    def test_best_log_detection_engine_node_works_correctly(self):
        engine_progress = EngineProgress('test')
        BestLogDetectionNode().run(engine_progress=engine_progress)

        log1 = BasicLog(self.wd.id, 'GK_D4417_D')
        log2 = BasicLog(self.wd.id, 'GK_D1800_D')

        self.assertEqual(True, log1.meta.best_log_detection['is_best'], msg='GK_D4412_D is the best log')
        self.assertEqual(False, log2.meta.best_log_detection['is_best'], msg='GK_D1800_D is not the best log')

    def test_score_log_tags(self):
        LOG_TAG_ASSESSMENT = read_best_log_tags_assessment()['General log tags']
        right_answer = sum(LOG_TAG_ASSESSMENT.values())
        answer = 0
        for tag in LOG_TAG_ASSESSMENT:
            tags = []
            tags.append(tag + '&' + tag)  # an unknown but looks similar to a known tag
            mixed_case = ''.join(letter.upper() if n % 2 else letter.lower() for n, letter in enumerate(tag))
            tags.append(mixed_case)  # a known tag, mixed case
            answer += score_log_tags(tags, LOG_TAG_ASSESSMENT)
        self.assertEqual(answer, right_answer)


class TestBestResistivityLogDetection(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self.wellname = 'WellTag'
        self.w = Well(self.wellname, new=True)
        self.dataset = 'Test'
        self.ds = WellDataset(self.w, self.dataset, new=True)

    def test_best_resistivity(self):
        logs_description = [
            ['GZ1', {'family': 'Lateral Resistivity', 'family_assigner': {'logging_service': 'DnM'}}, {'extra_shallow', 'unfocused'}],
            ['GZ2', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'DnM'}}, {'extra_shallow', 'unfocused'}],
            ['GZ3', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL'}}, {'extra_deep', 'unfocused'}],
            ['GZ4', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 10}}, {'extra_deep', 'unfocused', 'true'}],
            ['GZ5', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 20}}, {'shallow', 'unfocused', 'true'}],
            ['GZ6', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 20, 'vertical_resolution': 5}}, {'extra_deep', 'unfocused', 'true'}],
            ['GZ7', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 20, 'vertical_resolution': 3, 'frequency': 5}}, {'extra_deep', 'unfocused', 'true'}],
            ['GZ8', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 20, 'vertical_resolution': 3, 'frequency': 10}}, {'extra_deep', 'unfocused', 'true'}],
            ['GZ9', {'family': 'Resistivity', 'family_assigner': {'logging_service': 'WL', 'DOI': 20, 'vertical_resolution': 3, 'frequency': 10}}, {'extra_deep', 'focused', 'true'}]
        ]
        logs = []
        for log_name, meta, tags in logs_description:
            log = BasicLog(self.ds.id, log_name)
            log.values = np.array([[0., 10.], [1., 20.]])
            log.meta = meta
            log.meta.add_tags(*tags)
            log.save()
            logs.append(log)
        best_log = best_rt(logs)
        self.assertIsNotNone(best_log)
        self.assertEqual(best_log, logs[-1])
