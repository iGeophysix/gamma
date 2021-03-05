import os
import time
import unittest

from storage import ColumnStorage
from well import Well, WellDatasetColumns

PATH_TO_TEST_DATA = os.path.join('test_data')


class TestWell(unittest.TestCase):
    def setUp(self) -> None:
        _s = ColumnStorage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_well(self):
        w = Well("test_well", new=True)
        assert w.info == {}
        info = {"test_info": "test_key"}
        w.info = info
        assert w.info == info
        w.delete()
        assert str(w) == "test_well"

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
        dataset = WellDatasetColumns(well, "one")
        dataset.register()
        dataset.add_log(dataset_info.keys(), dataset_info.values())
        assert 'one' in well.datasets
        dataset.delete()
        assert 'one' not in well.datasets
        well.delete()