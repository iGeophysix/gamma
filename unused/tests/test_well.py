import os
import unittest

import psycopg2

from unused.columnstorage import ColumnStorage
from unused.well import Well, WellDataset

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../tests/test_data')


class TestWell(unittest.TestCase):
    def setUp(self) -> None:
        _s = ColumnStorage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_well(self):
        w = Well("test_well", new=True)
        self.assertEqual(w.info, {})
        info = {"test_info": "test_key"}
        w.info = info
        self.assertEqual(w.info, info)
        w.delete()
        self.assertEqual(str(w), "test_well")

    def test_create_well_and_add_dataset(self):
        wellname = "well2"
        dataset_info = {"GR": float, "PS": int, "COMMENT": str}
        test_data = {
            25: {"GR": 5, "PS": 10, "COMMENT": "Row 1"},
            25.5: {"GR": 5.5, "PS": -10, "COMMENT": "Row 2"},
            26: {"GR": 6, "PS": 10, "COMMENT": None},
            26.5: {"GR": 6.5, "PS": -10, "COMMENT": "cthulhu"},
        }

        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one")
        dataset.register()
        dataset.add_log(dataset_info.keys(), dataset_info.values())
        dataset.set_data(test_data)
        self.assertIn('one', well.datasets)
        dataset.delete()
        self.assertNotIn('one', well.datasets)
        well.delete()

    def test_insert_two_datasets_with_same_name(self):
        f = 'well1'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset1 = WellDataset(well, "one")
        dataset1.register()
        dataset2 = WellDataset(well, "one")
        with self.assertRaises(psycopg2.errors.DuplicateTable):
            dataset2.register()
