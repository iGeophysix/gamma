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
