import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestProject(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA
        self.files_to_test = ['another_small_file.las', 'small_file.las']
        for f in self.files_to_test:
            wellname = f[:-4]
            dataset_name = 'one'
            well = Well(wellname, True)
            dataset = WellDataset(well, dataset_name, True)

            las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                             well=well,
                             well_dataset=dataset)

    def test_tree(self):
        p = Project('test')
        tree = p.tree()
        for f in self.files_to_test:
            wellname = f[:-4]
            dataset_name = 'one'
            self.assertIn(wellname, tree)
            self.assertIn(dataset_name, tree[wellname])
            self.assertIn("GR", tree[wellname][dataset_name])

    def test_export_to_csv(self):
        p = Project('test')
        df = p.tree_df()
        df.to_csv('test_tree_df.csv', index=False)

        # TODO: check file

        os.remove('test_tree_df.csv')
