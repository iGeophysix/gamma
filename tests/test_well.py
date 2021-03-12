import os
import unittest

from storage import RedisStorage
from well import Well

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestWell(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_one_well(self):
        s = RedisStorage()

        w1 = Well('well1', new=True)
        wells_in_storage = s.list_wells()
        true_result = {'well1': {'name': 'well1', 'datasets': [], 'meta': {}}, }
        self.assertEqual(wells_in_storage, true_result)

    def test_create_two_well(self):
        s = RedisStorage()
        w1 = Well('well1', new=True)
        w2 = Well('well2', new=True)
        wells_in_storage = s.list_wells()
        true_result = {'well1': {'name': 'well1', 'datasets': [], 'meta': {}}, 'well2': {'name': 'well2', 'datasets': [], 'meta': {}}}
        self.assertEqual(wells_in_storage, true_result)

    def test_set_well_info(self):
        s = RedisStorage()
        w1 = Well('well1', new=True)

        info = w1.info
        info['uwi'] = '1324267832'
        w1.info = info

        wells_in_storage = s.list_wells()
        true_result = {'name': 'well1', 'datasets': [], 'meta': {'uwi': '1324267832'}}
        self.assertEqual(wells_in_storage['well1'], true_result)

    def test_check_no_datasets(self):
        w1 = Well('well1', new=True)
        self.assertEqual(w1.datasets, [])

    def test_check_name(self):
        w1 = Well('ratata', new=True)
        self.assertEqual(w1.name, 'ratata')

    def test_add_and_delete_well(self):
        w1 = Well('todelete', new=True)
        s = RedisStorage()
        wells_in_storage = s.list_wells()
        self.assertIn('todelete', wells_in_storage)
        w1.delete()
        wells_in_storage = s.list_wells()
        self.assertNotIn('todelete', wells_in_storage)
