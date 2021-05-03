import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las_import import LasImportNode


class TestLasImporter(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

    def test_las_import_works(self):
        folder = os.path.join(os.path.dirname(__file__), 'data')
        paths = list(map(lambda x: os.path.join(folder, x), os.listdir(folder)))
        node = LasImportNode()
        node.run([p for p in paths if p.endswith('.las')])

        p = Project()
        wells = p.list_wells()
        self.assertEqual(3, len(wells), "3 wells must be loaded")
        datasets = []
        for well in wells:
            datasets.extend(Well(well).datasets)
        self.assertEqual(3, len(datasets), "3 datasets must be loaded")

        w = Well('ANY ET AL A9-16-49-20')
        ds = WellDataset(w, 'sample_minimal.las')
        logs = ds.log_list
        self.assertEqual(7, len(logs), '7 logs in dataset must be loaded')


