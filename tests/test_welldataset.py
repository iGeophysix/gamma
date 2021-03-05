import os
import time
import unittest

from storage import ColumnStorage
from well import Well, WellDatasetColumns

PATH_TO_TEST_DATA = os.path.join('test_data')


class TestWellDatasetColumns(unittest.TestCase):
    def setUp(self) -> None:
        _s = ColumnStorage()
        _s.flush_db()
        _s.init_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_and_delete_datasets(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset = WellDatasetColumns(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        assert 'one' in well.datasets
        dataset = WellDatasetColumns(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        dataset = WellDatasetColumns(well, "one")
        dataset.delete()
        assert 'one' not in well.datasets
        assert 'two' in well.datasets

    def test_dataset_get_data(self):
        ref_depth = 2000.0880000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        data = dataset.get_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
        true_answer = {'GR': 46.731338501, 'SP': 63.879390717, 'DTC': 143.6020813, 'PEF': 6.3070640564, 'ROP': 38.207931519, 'RXO': None, 'CALI': 18.639200211,
                       'DRHO': 0.0151574211, 'NPHI': 0.5864710212, 'RDEP': 0.4988202751, 'RHOB': 1.8031704426, 'RMED': 0.4965194166, 'RSHA': None, 'X_LOC': 437627.5625,
                       'Y_LOC': 6470980.0, 'Z_LOC': -1974.846802, 'DEPTH_MD': 2000.0880127, 'MUDWEIGHT': 0.1366020888, 'FORCE_2020_LITHOFACIES_LITHOLOGY': 30000.0,
                       'FORCE_2020_LITHOFACIES_CONFIDENCE': 1.0}
        for key in true_answer.keys():
            assert data[ref_depth][key] == true_answer[key]

    def test_get_data_time(self):
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        start = time.time()
        data = dataset.get_data()
        end = time.time()
        assert end - start < 1

    def test_dataset_insert_row(self):
        wellname = 'random_well'
        dataset_name = 'insert_row'
        reference = 450
        reference_2 = 800
        row = {"GR": 87.81237987, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        row_2 = {"GR": 97.2, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        well = Well(wellname, new=True)
        dataset = WellDatasetColumns(well, dataset_name)
        dataset.register()
        dataset.add_log("GR", float)
        dataset.add_log("PS", float)
        dataset.add_log("LITHO", int)
        dataset.add_log("STRING", str)

        dataset.insert({reference: row})
        assert dataset.get_data(start=reference, end=reference) == {reference: row}
        dataset.insert({reference: row_2})
        assert dataset.get_data(start=reference, end=reference) == {reference: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference, end=reference) == {reference: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}

        dataset.insert({reference_2: row})
        assert dataset.get_data(logs=["GR", ], start=reference, end=reference) == {reference: {"GR": 97.2, }}
        dataset.insert({reference_2: row_2})
        assert dataset.get_data(start=reference_2, end=reference_2) == {reference_2: row_2}
        assert dataset.get_data(logs=["GR", "PS"], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, "PS": -0.234235555667, }}
        assert dataset.get_data(logs=["GR", ], start=reference_2, end=reference_2) == {reference_2: {"GR": 97.2, }}

    # def test_add_many_logs(self):
    #     log_count = 10
    #     LOG_TYPES = (float, str, int, bool, datetime,)
    #     wellname = 'thousand_logs'
    #     datasetname = 'this_dataset'
    #     wellname = Well(wellname, new=True)
    #     dataset = WellDatasetColumns(wellname, datasetname)
    #     # load some real data
    #     dataset.read_las(filename=os.path.join(self.path_to_test_data, f'15_9-15.las'))
    #
    #     # create logs in the dataset
    #     new_logs = [f"LOG_{l}" for l in range(0, log_count)]
    #     log_types = [LOG_TYPES[randint(0, len(LOG_TYPES) - 1)] for i in range(0, log_count)]
    #
    #     # get depths
    #     existing_data = dataset.get_data(logs=["reference", ])
    #
    #     # add data to the logs
    #     def dummy_data(dtype):
    #         generators = {
    #             float: 400 * random() - 200,
    #             str: ''.join(choice(string.ascii_letters) for i in range(64)),
    #             int: randint(-1000, 1000),
    #             datetime: datetime.now() + random() * timedelta(days=1),
    #             bool: 1 == randint(0, 1)
    #         }
    #         return generators[dtype]
    #
    #     def dummy_row(logs, dtypes):
    #         return {log: dummy_data(dtype) for log, dtype in zip(logs, dtypes)}
    #
    #     data = {depth: dummy_row(new_logs, log_types) for depth in existing_data.keys()}
    #
    #     start = time.time()
    #     dataset.add_log(new_logs, log_types)
    #     dataset.insert(data)
    #     end = time.time()
    #     print(f"Insertion of {log_count} logs took {int((end - start) * 1000)}ms")
    #
    #     for _ in range(5):
    #         start = time.time()
    #         d = dataset.get_data()
    #         end = time.time()
    #         print(f"Read of {len(d[25])} logs having {len(d)} rows took {int((end - start) * 1000)}ms.")
    #         print("Pause 5s")
    #         time.sleep(5)
    #
    # def test_add_many_logs_read_ten_random(self):
    #     log_count = 10
    #     LOG_TYPES = (float, str, int, bool, datetime,)
    #     wellname = 'thousand_logs'
    #     datasetname = 'this_dataset'
    #     wellname = Well(wellname, new=True)
    #     dataset = WellDatasetColumns(wellname, datasetname)
    #     # load some real data
    #     dataset.read_las(filename=os.path.join(self.path_to_test_data, f'15_9-15.las'))
    #
    #     # create logs in the dataset
    #     new_logs = [f"LOG_{l}" for l in range(0, log_count)]
    #     log_types = [LOG_TYPES[randint(0, len(LOG_TYPES) - 1)] for i in range(0, log_count)]
    #
    #     # get depths
    #     existing_data = dataset.get_data(logs=["reference", ])
    #
    #     # add data to the logs
    #     def dummy_data(dtype):
    #         generators = {
    #             float: 400 * random() - 200,
    #             str: ''.join(choice(string.ascii_letters) for i in range(64)),
    #             int: randint(-1000, 1000),
    #             datetime: datetime.now() + random() * timedelta(days=1),
    #             bool: 1 == randint(0, 1)
    #         }
    #         return generators[dtype]
    #
    #     def dummy_row(logs, dtypes):
    #         return {log: dummy_data(dtype) for log, dtype in zip(logs, dtypes)}
    #
    #     data = {depth: dummy_row(new_logs, log_types) for depth in existing_data.keys()}
    #
    #     start = time.time()
    #     dataset.add_log(new_logs, log_types)
    #     dataset.insert(data)
    #     end = time.time()
    #     print(f"Insertion of {log_count} logs took {int((end - start) * 1000)}ms")
    #
    #     for _ in range(5):
    #         start = time.time()
    #         d = dataset.get_data(logs=[new_logs[randint(0, len(new_logs) - 1)] for _ in range(0, 10)])
    #         end = time.time()
    #         print(f"Read of {len(d[25])} logs having {len(d)} rows took {int((end - start) * 1000)}ms.")
    #         print("Pause 5s")
    #         time.sleep(5)
