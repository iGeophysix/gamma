import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.MarkersSet import MarkersSet
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.markers_importexport import import_markers_csv
from settings import BASE_DIR, DEFAULT_MARKERS_NAME


class TestMarkersLog(unittest.TestCase):
    def setUp(self) -> None:
        self.path_to_test_data = os.path.join(BASE_DIR, 'test_data')
        _s = RedisStorage()
        # _s.flush_db()

    def test_markers_without_gaps_loading_from_csv(self):
        with open(os.path.join(self.path_to_test_data, 'Markers', 'FieldData_StratigraphyWithoutGaps.csv')) as raw_data:
            import_markers_csv(raw_data=raw_data, missing_value='-9999')

        w = Well('100')
        ds = WellDataset(w, DEFAULT_MARKERS_NAME)
        self.assertIn('Stratigraphy_comb', ds.log_list)

    def test_markers_with_gaps_loading_from_csv(self):
        with open(os.path.join(self.path_to_test_data, 'Markers', 'FieldData_StratigraphyWithGaps.csv')) as raw_data:
            import_markers_csv(raw_data=raw_data, missing_value='-9999')

        w = Well('100')
        ds = WellDataset(w, DEFAULT_MARKERS_NAME)
        self.assertIn('Stratigraphy', ds.log_list)

    def test_markerset_well_ids_index(self):
        with open(os.path.join(self.path_to_test_data, 'Markers', 'FieldData_StratigraphyWithoutGaps.csv')) as raw_data:
            import_markers_csv(raw_data=raw_data, missing_value='-9999')

        ms = MarkersSet('Stratigraphy_comb')
        self.assertEqual(36, len(ms.well_ids))

        w = Well('100')
        ds = WellDataset(w, DEFAULT_MARKERS_NAME)
        ds.delete_log('Stratigraphy_comb')
        self.assertEqual(35, len(ms.well_ids))
