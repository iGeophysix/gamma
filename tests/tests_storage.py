import unittest

from storage import ColumnStorage


class TestStorage(unittest.TestCase):
    def setUp(self) -> None:
        self._s = ColumnStorage()
        self._s.flush_db()
        self._s.init_db()

    def test_create_well(self):
        self._s.create_well("test_well")
        assert self._s.get_well_info("test_well") == {}
        info = {"name": "TEST WELL 2", "other_key":2}
        self._s.update_well_info("test_well_2", info)
        assert self._s.get_well_info("test_well_2") == info

    def test_delete_well(self):
        self._s.create_well("test_well")
        assert self._s.get_well_info("test_well") == {}
        self._s.delete_well("test_well")
        assert "test_well" not in self._s.list_wells()


