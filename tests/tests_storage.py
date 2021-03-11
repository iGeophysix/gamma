import unittest

from storage import RedisStorage


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self._s.init_db()

    def test_create_well(self):
        self._s.create_well("test_well")
        self.assertEqual(self._s.get_well_info("test_well"), {})
        info = {"wellname": "TEST WELL 2", "other_key": 2}
        self._s.update_well_info("test_well_2", info)
        self.assertEqual(self._s.get_well_info("test_well_2"), info)
        self._s.delete_well("test_well")

    def test_delete_well(self):
        self._s.create_well("test_well")
        self.assertEqual(self._s.get_well_info("test_well"), {})
        self._s.delete_well("test_well")
        self.assertNotIn("test_well", self._s.list_wells())
