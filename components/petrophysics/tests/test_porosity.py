import os
import unittest

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.porosity import linear_method
from tasks import async_get_basic_log_stats, async_read_las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestShaleVolume(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '15-9-19'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'Volve_15-9-19_PHIT_D.las'
        self.wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

    def test_porosity_linear_works_correctly(self):
        gk = BasicLog(self.wd.id, "RHOB")
        rhob_fluid = 1.05  # g/cm3
        rhob_matrix = 2.65  # g/cm3
        phit_d = linear_method(gk, rhob_fluid=rhob_fluid, rhob_matrix=rhob_matrix)
        phit_d.dataset_id = self.wd.id
        phit_d.name = "PHIT_D"
        phit_d.save()

        true_phit_d = BasicLog(self.wd.id, "PHIT_D_TRUE")
        diff = abs(phit_d.values[:, 1] - true_phit_d[:, 1])
        self.assertAlmostEqual(0.0, np.nanmax(diff, ), 4)
