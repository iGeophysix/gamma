import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well, get_well_by_id
from components.domain.WellDataset import WellDataset, get_dataset_by_id
from components.importexport.las import import_to_db
from components.petrophysics.curve_operations import get_basic_curve_statistics
from components.petrophysics.normalization import log_normalization, LogNormalizationNode
from tasks import async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data', 'normalization')


class TestLogNormalization(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()

        for filename in os.listdir(PATH_TO_TEST_DATA):
            import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

        p = Project()
        for wellname in p.list_wells().keys():
            async_get_basic_log_stats(wellname)

    def test_log_normalization_works_correctly(self):
        logs_to_normalize = []
        p = Project()
        for wellname in p.list_wells().keys():
            w = Well(wellname)
            for datasetname in w.datasets:
                log_id = 'GR'
                ds = WellDataset(w, datasetname)
                logs_to_normalize.append(BasicLog(ds.id, log_id))

        log_norm = LogNormalizationNode()
        normalized_logs = log_norm.run(logs_to_normalize)
        true_q5 = 5.59728
        true_q95 = 26.38376
        for log in normalized_logs:
            dataset = get_dataset_by_id(log.dataset_id)
            well = Well(dataset.well)
            target_wd = WellDataset(well, 'Normalized', new=True)
            log._dataset_id = target_wd.id

            log.meta.basic_statistics = get_basic_curve_statistics(log.values)
            log.save()
            log.meta.append_history(f"Normalized from {well.name}-{dataset.name}-{log.name}")
            log.save()
            q5 = np.quantile(log.values[~np.isnan(log.values[:, 1])][:, 1], 0.05)
            q95 = np.quantile(log.values[~np.isnan(log.values[:, 1])][:, 1], 0.95)
            self.assertAlmostEqual(true_q5, q5, places=4)
            self.assertAlmostEqual(true_q95, q95, places=4)
