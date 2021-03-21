import multiprocessing as mp
import os
import unittest

from datetime import datetime

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset

from components.importexport import las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


def read(well, dataset, log):
    w = Well(well)
    d = WellDataset(w, dataset)
    data = d.get_log_meta([log, ])
    return data


def set_data(well, dataset, data, meta):
    w = Well(well)
    d = WellDataset(w, dataset)
    d.set_data(data, meta)


def append_history(well, dataset, log, event_text):
    w = Well(well)
    d = WellDataset(w, dataset)
    d.append_log_history(log, (datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"), event_text))


class TestParallelAccessToData(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA

        self.f = 'another_small_file.las'
        self.wellname = self.f[:-4]
        self.number_of_datasets = 100
        well = Well(self.wellname, new=True)
        for i in range(self.number_of_datasets):
            dataset = WellDataset(well, str(i), new=True)

            las.import_to_db(filename=os.path.join(self.path_to_test_data, self.f),
                             well=well,
                             well_dataset=dataset)

    def test_parallel_read(self):
        pool = mp.Pool(4)
        results = []
        for i in range(self.number_of_datasets):
            results.append(pool.apply_async(read, (self.wellname, str(i), 'GR')))
        pool.close()
        pool.join()
        for r in results:
            self.assertIn("GR", r.get().keys())

    def test_parallel_set_data_different_datasets(self):
        pool = mp.Pool(4)

        data = {"GR_1": {100: 10, 200: 20}}
        meta = {"GR_1": {"units": 'gAPI', 'code': '--'}}

        for i in range(self.number_of_datasets):
            pool.apply_async(set_data, (self.wellname, str(i), data, meta))
        pool.close()
        pool.join()

        well = Well(self.wellname)
        for i in range(self.number_of_datasets):
            d = WellDataset(well, str(i))
            dl = d.get_log_data(logs=['GR_1', ])
            self.assertEqual(dl, data)

    def test_parallel_set_data_same_dataset(self):
        pool = mp.Pool(4)

        data = {"GR_1": {100: 10, 200: 20}}
        meta = {"GR_1": {"units": 'gAPI', 'code': '--'}}

        for i in range(self.number_of_datasets):
            pool.apply_async(set_data, (self.wellname, "1", data, meta))
        pool.close()
        pool.join()

        well = Well(self.wellname)
        for i in range(self.number_of_datasets):
            d = WellDataset(well, "1")
            dl = d.get_log_data(logs=['GR_1', ])
            self.assertEqual(dl, data)

    def test_parallel_append_history(self):
        pool = mp.Pool(4)
        number_of_writes = 20

        for i in range(number_of_writes):
            pool.apply_async(append_history, (self.wellname, "1", "GR", f"This is append {i}"))
        pool.close()
        pool.join()

        well = Well(self.wellname)

        d = WellDataset(well, "1")
        dl = d.get_log_history(log='GR')
        self.assertEqual(len(dl), number_of_writes + 1)  # one is added at the beginning when loading data from las
