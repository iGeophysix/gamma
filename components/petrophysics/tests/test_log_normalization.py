import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las import import_to_db
from components.petrophysics.curve_operations import get_basic_curve_statistics
from components.petrophysics.normalization import log_normalization
from tasks import async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data', 'normalization')


class TestLogNormalization(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()

        for filename in os.listdir(PATH_TO_TEST_DATA):
            import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

        p = Project('test')
        for wellname in p.list_wells().keys():
            async_get_basic_log_stats(wellname)

    def test_log_normalization_works_correctly(self):
        logs_to_normalize = []
        p = Project('test')
        for wellname in p.list_wells().keys():
            w = Well(wellname)
            for datasetname in w.datasets:
                logname = 'GR'
                logs_to_normalize.append((wellname, datasetname, logname))

        normalized_data, normalized_meta = log_normalization(logs_to_normalize)
        true_q5 = 5.59728
        true_q95 = 26.38376
        for w_wd_log, values in normalized_data.items():
            w, wd, log = w_wd_log
            well = Well(w)
            wd = WellDataset(well, 'Normalized', new=True)

            normalized_meta[w_wd_log].update({'basic_statistics': get_basic_curve_statistics(values)})
            wd.set_data(data={log: values}, meta={log: normalized_meta[w_wd_log]})
            wd.append_log_history(log, f"Normalized from {w_wd_log}")
            q5 = np.quantile(values[:, 1], 0.05)
            q95 = np.quantile(values[:, 1], 0.95)
            self.assertAlmostEqual(true_q5, q5, places=4)
            self.assertAlmostEqual(true_q95, q95, places=4)