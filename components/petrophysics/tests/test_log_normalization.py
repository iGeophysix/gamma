import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las import import_to_db
from components.petrophysics.curve_operations import get_basic_curve_statistics
from components.petrophysics.normalization import LogNormalizationNode
from settings import BASE_DIR
from tasks import async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics', 'normalization')


class TestLogNormalization(unittest.TestCase):
    def setUp(self) -> None:
        # pass
        self._s = RedisStorage()
        self._s.flush_db()

        for filename in os.listdir(PATH_TO_TEST_DATA):
            import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

        p = Project()
        for wellname in p.list_wells().keys():
            async_get_basic_log_stats(wellname)
            w = Well(wellname)
            for datasetname in w.datasets:
                log_id = 'GR'
                ds = WellDataset(w, datasetname)
                log = BasicLog(ds.id, log_id)
                log.meta.family = 'Gamma Ray'
                log.save()

    def test_log_normalization_works_correctly(self):
        log_norm = LogNormalizationNode()
        log_norm.run(lower_quantile=0.05, upper_quantile=0.95)

        true_q5 = 5.59728
        true_q95 = 26.38376

        p = Project()
        for wellname in p.list_wells().keys():
            async_get_basic_log_stats(wellname)
            w = Well(wellname)
            ds = WellDataset(w, 'LQC')
            log_id = 'GR'
            log = BasicLog(ds.id, log_id)
            log.meta.basic_statistics = get_basic_curve_statistics(log.values)

            q5 = np.quantile(log.values[~np.isnan(log.values[:, 1])][:, 1], 0.05)
            q95 = np.quantile(log.values[~np.isnan(log.values[:, 1])][:, 1], 0.95)
            self.assertAlmostEqual(true_q5, q5, places=4)
            self.assertAlmostEqual(true_q95, q95, places=4)
