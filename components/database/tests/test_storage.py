import unittest

from components.database.RedisStorage import RedisStorage


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()

    def test_create_well(self):
        self._s.create_well("test_well")
        self.assertEqual(self._s.get_well_info("test_well"), {})
        info = {"wellname": "TEST WELL 2", "other_key": 2}
        self._s.set_well_info("test_well", info)
        self.assertEqual(self._s.get_well_info("test_well"), info)
        self._s.delete_well("test_well")

    def test_delete_well(self):
        self._s.create_well("test_well")
        self.assertEqual(self._s.get_well_info("test_well"), {})
        self._s.delete_well("test_well")
        self.assertNotIn("test_well", self._s.list_wells())

    def test_append_log_meta(self):
        wellname = "test_well"
        datasetname = "test_dataset"
        logname = "GR"
        self._s.create_well(wellname)
        self._s.create_dataset(wellname, datasetname)
        dataset_id = self._s._get_dataset_id(wellname, datasetname)
        self._s.add_log(dataset_id, logname)
        current_meta = self._s.get_logs_meta(dataset_id, [logname, ])
        self.assertEqual({'GR': {}}, current_meta)

        extra_meta = {"UWI": 434232}
        self._s.append_log_meta(dataset_id, logname, extra_meta)
        self.assertEqual({'GR': {"UWI": 434232}}, self._s.get_logs_meta(dataset_id, [logname, ]))

        extra_meta = {"PWA": "GOGI"}
        self._s.append_log_meta(dataset_id, logname, extra_meta)
        self.assertEqual({'GR': {"UWI": 434232, "PWA": "GOGI"}}, self._s.get_logs_meta(dataset_id, [logname, ]))

        extra_meta = {"PWA": "GIGI"}
        self._s.append_log_meta(dataset_id, logname, extra_meta)
        self.assertEqual({'GR': {"UWI": 434232, "PWA": "GIGI"}}, self._s.get_logs_meta(dataset_id, [logname, ]))

    def test_markerset_interface(self):
        markerset1 = {
            'name': 'MarkerSet1',
            'markers': {"ZoneA1": 1, "ZoneB1": 2, "ZoneC1": 3},
            '_sequence_max': 4,
        }
        markerset2 = {
            'name': 'MarkerSet2',
            'markers': {"ZoneA2": 1, "ZoneB2": 2, "ZoneC2": 3},
            '_sequence_max': 4,
        }
        self.assertEqual([], self._s.list_markersets(), 'Should be no markersets')

        self._s.set_markerset_by_name(markerset1)
        self.assertEqual(['MarkerSet1'], self._s.list_markersets(), 'Should be one markerset: MarkerSet1')

        self._s.set_markerset_by_name(markerset2)
        self.assertCountEqual(['MarkerSet1', 'MarkerSet2'], self._s.list_markersets(), 'Should be two markersets: MarkerSet1,MarkerSet1 ')

        ms_raw = self._s.get_markerset_by_name('MarkerSet1')
        self.assertEqual(markerset1, ms_raw, 'Check MarkerSet1 content')

        self._s.delete_markerset_by_name('MarkerSet1')
        self.assertEqual(['MarkerSet2'], self._s.list_markersets(), 'Should be one markerset: MarkerSet2')

