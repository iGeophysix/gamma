import json
import os
import string
import time
import unittest
from datetime import datetime, timedelta
from random import randint, random, choice

import numpy as np

from storage import RedisStorage
from tasks_redis import async_read_las, async_normalize_log
from well_redis import Well, WellDataset

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestWellDatasetRedis(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_and_delete_datasets(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        dataset = WellDataset(well, "one")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        self.assertIn('one', well.datasets)
        dataset = WellDataset(well, "two")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f))
        dataset = WellDataset(well, "one")
        dataset.delete()
        self.assertNotIn('one', well.datasets)
        self.assertIn('two', well.datasets)

    def test_dataset_get_data(self):
        ref_depth = 2000.0880000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'), )
        data = dataset.get_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
        true_answer = {'GR': 46.731338501, 'SP': 63.879390717, 'DTC': 143.6020813, 'PEF': 6.3070640564, 'ROP': 38.207931519, 'RXO': np.nan, 'CALI': 18.639200211,
                       'DRHO': 0.0151574211, 'NPHI': 0.5864710212, 'RDEP': 0.4988202751, 'RHOB': 1.8031704426, 'RMED': 0.4965194166, 'RSHA': np.nan, 'X_LOC': 437627.5625,
                       'Y_LOC': 6470980.0, 'Z_LOC': -1974.846802, 'DEPTH_MD': 2000.0880127, 'MUDWEIGHT': 0.1366020888, 'FORCE_2020_LITHOFACIES_LITHOLOGY': 30000.0,
                       'FORCE_2020_LITHOFACIES_CONFIDENCE': 1.0}
        for key in true_answer.keys():
            self.assertTrue(data[key][ref_depth] == true_answer[key] or (np.isnan(data[key][ref_depth]) and np.isnan(true_answer[key])))

    @unittest.skip("Is not a test in fact")
    def test_get_data_time(self):
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        start = time.time()
        dataset.get_data()
        end = time.time()
        self.assertLess(end - start, 1)

    def test_dataset_insert_row(self):
        wellname = 'random_well'
        dataset_name = 'insert_row'
        reference = 450
        reference_2 = 800
        row = {"GR": 87.81237987, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}
        row_2 = {"GR": 97.2, "PS": -0.234235555667, "LITHO": 1, "STRING": "VALUE"}

        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.register()

        def assemble_data(reference, row):
            return {l: json.dumps({reference: v}) for l, v in row.items()}

        dataset.set_data(assemble_data(reference, row))
        self.assertEqual(dataset.get_data(start=reference, end=reference),
                         {'GR': {450.0: 87.81237987}, 'PS': {450.0: -0.234235555667}, 'LITHO': {450.0: 1}, 'STRING': {450.0: 'VALUE'}})
        dataset.set_data(assemble_data(reference, row_2))
        self.assertEqual(dataset.get_data(start=reference, end=reference), {'GR': {450.0: 97.2}, 'PS': {450.0: -0.234235555667}, 'LITHO': {450.0: 1}, 'STRING': {450.0: 'VALUE'}})
        self.assertEqual(dataset.get_data(logs=["GR", "PS"], start=reference, end=reference), {'GR': {450.0: 97.2}, 'PS': {450.0: -0.234235555667}})
        self.assertEqual(dataset.get_data(logs=["GR", ], start=reference, end=reference), {"GR": {450.0: 97.2, }})

    def test_check_las_header(self):
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{wellname}.las'))
        true_info = {
            "Well": {"API": {"unit": "", "descr": "API NUMBER", "value": "", "mnemonic": "API", "original_mnemonic": "API"},
                     "FLD": {"unit": "", "descr": "FIELD", "value": "", "mnemonic": "FLD", "original_mnemonic": "FLD"},
                     "LOC": {"unit": "", "descr": "LOCATION", "value": "", "mnemonic": "LOC", "original_mnemonic": "LOC"},
                     "UWI": {"unit": "", "descr": "UNIQUE WELL ID", "value": "15/9-13", "mnemonic": "UWI", "original_mnemonic": "UWI"},
                     "COMP": {"unit": "", "descr": "COMPANY", "value": "", "mnemonic": "COMP", "original_mnemonic": "COMP"},
                     "DATE": {"unit": "", "descr": "ss}", "value": "2020-08-09 20:01:10   : Log Export Date {yyyy-MM-dd HH:mm", "mnemonic": "DATE",
                              "original_mnemonic": "DATE"}, "NULL": {"unit": "", "descr": "", "value": -999.25, "mnemonic": "NULL", "original_mnemonic": "NULL"},
                     "PROV": {"unit": "", "descr": "PROVINCE", "value": "", "mnemonic": "PROV", "original_mnemonic": "PROV"},
                     "SRVC": {"unit": "", "descr": "SERVICE COMPANY", "value": "", "mnemonic": "SRVC", "original_mnemonic": "SRVC"},
                     "STEP": {"unit": "m", "descr": "", "value": 0.152, "mnemonic": "STEP", "original_mnemonic": "STEP"},
                     "STOP": {"unit": "m", "descr": "", "value": 3283.9641113, "mnemonic": "STOP", "original_mnemonic": "STOP"},
                     "STRT": {"unit": "m", "descr": "", "value": 25.0, "mnemonic": "STRT", "original_mnemonic": "STRT"},
                     "WELL": {"unit": "", "descr": "WELL", "value": "15/9-13 Sleipner East Appr", "mnemonic": "WELL", "original_mnemonic": "WELL"}}, "Other": "",
            "Curves": {"GR": {"unit": "gAPI", "descr": "GR", "value": "", "mnemonic": "GR", "original_mnemonic": "GR"},
                       "SP": {"unit": "mV", "descr": "SP", "value": "", "mnemonic": "SP", "original_mnemonic": "SP"},
                       "DTC": {"unit": "us/ft", "descr": "DTC", "value": "", "mnemonic": "DTC", "original_mnemonic": "DTC"},
                       "PEF": {"unit": "b/e", "descr": "PEF", "value": "", "mnemonic": "PEF", "original_mnemonic": "PEF"},
                       "ROP": {"unit": "m/h", "descr": "ROP", "value": "", "mnemonic": "ROP", "original_mnemonic": "ROP"},
                       "RXO": {"unit": "ohm.m", "descr": "RXO", "value": "", "mnemonic": "RXO", "original_mnemonic": "RXO"},
                       "CALI": {"unit": "in", "descr": "CALI", "value": "", "mnemonic": "CALI", "original_mnemonic": "CALI"},
                       "DEPT": {"unit": "m", "descr": "DEPTH", "value": "", "mnemonic": "DEPT", "original_mnemonic": "DEPT"},
                       "DRHO": {"unit": "g/cm3", "descr": "DRHO", "value": "", "mnemonic": "DRHO", "original_mnemonic": "DRHO"},
                       "NPHI": {"unit": "m3/m3", "descr": "NPHI", "value": "", "mnemonic": "NPHI", "original_mnemonic": "NPHI"},
                       "RDEP": {"unit": "ohm.m", "descr": "RDEP", "value": "", "mnemonic": "RDEP", "original_mnemonic": "RDEP"},
                       "RHOB": {"unit": "g/cm3", "descr": "RHOB", "value": "", "mnemonic": "RHOB", "original_mnemonic": "RHOB"},
                       "RMED": {"unit": "ohm.m", "descr": "RMED", "value": "", "mnemonic": "RMED", "original_mnemonic": "RMED"},
                       "RSHA": {"unit": "ohm.m", "descr": "RSHA", "value": "", "mnemonic": "RSHA", "original_mnemonic": "RSHA"},
                       "X_LOC": {"unit": "_", "descr": "x_loc", "value": "", "mnemonic": "X_LOC", "original_mnemonic": "X_LOC"},
                       "Y_LOC": {"unit": "_", "descr": "y_loc", "value": "", "mnemonic": "Y_LOC", "original_mnemonic": "Y_LOC"},
                       "Z_LOC": {"unit": "_", "descr": "z_loc", "value": "", "mnemonic": "Z_LOC", "original_mnemonic": "Z_LOC"},
                       "DEPTH_MD": {"unit": "_", "descr": "DEPTH_MD", "value": "", "mnemonic": "DEPTH_MD", "original_mnemonic": "DEPTH_MD"},
                       "MUDWEIGHT": {"unit": "_", "descr": "MUDWEIGHT", "value": "", "mnemonic": "MUDWEIGHT", "original_mnemonic": "MUDWEIGHT"},
                       "FORCE_2020_LITHOFACIES_LITHOLOGY": {"unit": "_", "descr": "FORCE_2020_LITHOFACIES_LITHOLOGY", "value": "", "mnemonic": "FORCE_2020_LITHOFACIES_LITHOLOGY",
                                                            "original_mnemonic": "FORCE_2020_LITHOFACIES_LITHOLOGY"},
                       "FORCE_2020_LITHOFACIES_CONFIDENCE": {"unit": "_", "descr": "FORCE_2020_LITHOFACIES_CONFIDENCE", "value": "",
                                                             "mnemonic": "FORCE_2020_LITHOFACIES_CONFIDENCE", "original_mnemonic": "FORCE_2020_LITHOFACIES_CONFIDENCE"}},
            "Version": {"VERS": {"unit": "", "descr": "", "value": 2.0, "mnemonic": "VERS", "original_mnemonic": "VERS"},
                        "WRAP": {"unit": "", "descr": "", "value": "NO", "mnemonic": "WRAP", "original_mnemonic": "WRAP"}},
            "Parameter": {}}
        self.assertEqual(true_info, dataset.info)
        well.delete()

    def test_set_las_header(self):
        wellname = '15_9-13'
        dataset_name = 'three'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name)
        dataset.register()
        true_info = {
            "Well": {"API": {"unit": "", "descr": "API NUMBER", "value": "", "mnemonic": "API", "original_mnemonic": "API"},
                     "FLD": {"unit": "", "descr": "FIELD", "value": "", "mnemonic": "FLD", "original_mnemonic": "FLD"},
                     "LOC": {"unit": "", "descr": "LOCATION", "value": "", "mnemonic": "LOC", "original_mnemonic": "LOC"},
                     "UWI": {"unit": "", "descr": "UNIQUE WELL ID", "value": "15/9-13", "mnemonic": "UWI", "original_mnemonic": "UWI"},
                     "COMP": {"unit": "", "descr": "COMPANY", "value": "", "mnemonic": "COMP", "original_mnemonic": "COMP"},
                     "DATE": {"unit": "", "descr": "ss}", "value": "2020-08-09 20:01:10   : Log Export Date {yyyy-MM-dd HH:mm", "mnemonic": "DATE",
                              "original_mnemonic": "DATE"}, "NULL": {"unit": "", "descr": "", "value": -999.25, "mnemonic": "NULL", "original_mnemonic": "NULL"},
                     "PROV": {"unit": "", "descr": "PROVINCE", "value": "", "mnemonic": "PROV", "original_mnemonic": "PROV"},
                     "SRVC": {"unit": "", "descr": "SERVICE COMPANY", "value": "", "mnemonic": "SRVC", "original_mnemonic": "SRVC"},
                     "STEP": {"unit": "m", "descr": "", "value": 0.152, "mnemonic": "STEP", "original_mnemonic": "STEP"},
                     "STOP": {"unit": "m", "descr": "", "value": 3283.9641113, "mnemonic": "STOP", "original_mnemonic": "STOP"},
                     "STRT": {"unit": "m", "descr": "", "value": 25.0, "mnemonic": "STRT", "original_mnemonic": "STRT"},
                     "WELL": {"unit": "", "descr": "WELL", "value": "15/9-13 Sleipner East Appr", "mnemonic": "WELL", "original_mnemonic": "WELL"}}, "Other": "",
            "Curves": {"GR": {"unit": "gAPI", "descr": "GR", "value": "", "mnemonic": "GR", "original_mnemonic": "GR"},
                       "SP": {"unit": "mV", "descr": "SP", "value": "", "mnemonic": "SP", "original_mnemonic": "SP"},
                       "DTC": {"unit": "us/ft", "descr": "DTC", "value": "", "mnemonic": "DTC", "original_mnemonic": "DTC"},
                       "PEF": {"unit": "b/e", "descr": "PEF", "value": "", "mnemonic": "PEF", "original_mnemonic": "PEF"},
                       "ROP": {"unit": "m/h", "descr": "ROP", "value": "", "mnemonic": "ROP", "original_mnemonic": "ROP"},
                       "RXO": {"unit": "ohm.m", "descr": "RXO", "value": "", "mnemonic": "RXO", "original_mnemonic": "RXO"},
                       "CALI": {"unit": "in", "descr": "CALI", "value": "", "mnemonic": "CALI", "original_mnemonic": "CALI"},
                       "DEPT": {"unit": "m", "descr": "DEPTH", "value": "", "mnemonic": "DEPT", "original_mnemonic": "DEPT"},
                       "DRHO": {"unit": "g/cm3", "descr": "DRHO", "value": "", "mnemonic": "DRHO", "original_mnemonic": "DRHO"},
                       "NPHI": {"unit": "m3/m3", "descr": "NPHI", "value": "", "mnemonic": "NPHI", "original_mnemonic": "NPHI"},
                       "RDEP": {"unit": "ohm.m", "descr": "RDEP", "value": "", "mnemonic": "RDEP", "original_mnemonic": "RDEP"},
                       "RHOB": {"unit": "g/cm3", "descr": "RHOB", "value": "", "mnemonic": "RHOB", "original_mnemonic": "RHOB"},
                       "RMED": {"unit": "ohm.m", "descr": "RMED", "value": "", "mnemonic": "RMED", "original_mnemonic": "RMED"},
                       "RSHA": {"unit": "ohm.m", "descr": "RSHA", "value": "", "mnemonic": "RSHA", "original_mnemonic": "RSHA"},
                       "X_LOC": {"unit": "_", "descr": "x_loc", "value": "", "mnemonic": "X_LOC", "original_mnemonic": "X_LOC"},
                       "Y_LOC": {"unit": "_", "descr": "y_loc", "value": "", "mnemonic": "Y_LOC", "original_mnemonic": "Y_LOC"},
                       "Z_LOC": {"unit": "_", "descr": "z_loc", "value": "", "mnemonic": "Z_LOC", "original_mnemonic": "Z_LOC"},
                       "DEPTH_MD": {"unit": "_", "descr": "DEPTH_MD", "value": "", "mnemonic": "DEPTH_MD", "original_mnemonic": "DEPTH_MD"},
                       "MUDWEIGHT": {"unit": "_", "descr": "MUDWEIGHT", "value": "", "mnemonic": "MUDWEIGHT", "original_mnemonic": "MUDWEIGHT"},
                       "FORCE_2020_LITHOFACIES_LITHOLOGY": {"unit": "_", "descr": "FORCE_2020_LITHOFACIES_LITHOLOGY", "value": "", "mnemonic": "FORCE_2020_LITHOFACIES_LITHOLOGY",
                                                            "original_mnemonic": "FORCE_2020_LITHOFACIES_LITHOLOGY"},
                       "FORCE_2020_LITHOFACIES_CONFIDENCE": {"unit": "_", "descr": "FORCE_2020_LITHOFACIES_CONFIDENCE", "value": "",
                                                             "mnemonic": "FORCE_2020_LITHOFACIES_CONFIDENCE", "original_mnemonic": "FORCE_2020_LITHOFACIES_CONFIDENCE"}},
            "Version": {"VERS": {"unit": "", "descr": "", "value": 2.0, "mnemonic": "VERS", "original_mnemonic": "VERS"},
                        "WRAP": {"unit": "", "descr": "", "value": "NO", "mnemonic": "WRAP", "original_mnemonic": "WRAP"}},
            "Parameter": {}}
        dataset.info = true_info
        self.assertEqual(true_info, dataset.info)
        well.delete()

    @unittest.skip("Takes too long")
    def test_add_many_logs(self):
        log_count = 50
        LOG_TYPES = (float, str, int, bool, datetime,)
        wellname = 'thousand_logs'
        datasetname = 'this_dataset'
        wellname = Well(wellname, new=True)
        dataset = WellDataset(wellname, datasetname)
        # load some real data
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'15_9-15.las'))

        # create logs in the dataset
        new_logs = {f"LOG_{i}": LOG_TYPES[randint(0, len(LOG_TYPES) - 1)] for i in range(0, log_count)}

        # get depths
        existing_depths = dataset.get_data(logs=["GR", ])["GR"].keys()

        # add data to the logs
        def dummy_data(dtype):
            generators = {
                float: 400 * random() - 200,
                str: ''.join(choice(string.ascii_letters) for i in range(64)),
                int: randint(-1000, 1000),
                datetime: datetime.strftime(datetime.now() + random() * timedelta(days=1), "%Y-%m-%d %H:%M:%S.%f%z"),
                bool: 1 == randint(0, 1)
            }
            return generators[dtype]

        def dummy_row(depths, dtype):
            return {depth: dummy_data(dtype) for depth in depths}

        data = {log: json.dumps(dummy_row(existing_depths, log_type)) for log, log_type in new_logs.items()}

        start = time.time()
        # dataset.add_log(new_logs, log_types)
        dataset.set_data(data)
        end = time.time()
        print(f"Insertion of {log_count} logs took {int((end - start) * 1000)}ms")

        for _ in range(5):
            start = time.time()
            d = dataset.get_data()
            end = time.time()
            print(f"Read of {len(d)} logs having {len(d['GR'])} rows took {int((end - start) * 1000)}ms.")
            time.sleep(1)

    @unittest.skip("Takes too long")
    def test_add_many_logs_read_random(self):
        log_count = 5
        LOG_TYPES = (float, str, int, bool, datetime,)
        wellname = 'thousand_logs'
        datasetname = 'this_dataset'
        wellname = Well(wellname, new=True)
        dataset = WellDataset(wellname, datasetname)
        # load some real data
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'15_9-15.las'))

        # create logs in the dataset
        new_logs = [f"LOG_{l}" for l in range(0, log_count)]
        log_types = [LOG_TYPES[randint(0, len(LOG_TYPES) - 1)] for i in range(0, log_count)]

        # get depths
        existing_data = dataset.get_data(logs=["reference", ])

        # add data to the logs
        def dummy_data(dtype):
            generators = {
                float: 400 * random() - 200,
                str: ''.join(choice(string.ascii_letters) for i in range(64)),
                int: randint(-1000, 1000),
                datetime: datetime.now() + random() * timedelta(days=1),
                bool: 1 == randint(0, 1)
            }
            return generators[dtype]

        def dummy_row(logs, dtypes):
            return {log: dummy_data(dtype) for log, dtype in zip(logs, dtypes)}

        data = {depth: dummy_row(new_logs, log_types) for depth in existing_data.keys()}

        start = time.time()
        dataset.set_data(data)
        end = time.time()
        print(f"Insertion of {log_count} logs took {int((end - start) * 1000)}ms")

        for _ in range(3):
            start = time.time()
            d = dataset.get_data(logs=[new_logs[randint(0, len(new_logs) - 1)] for _ in range(0, 10)])
            end = time.time()
            print(f"Read of {len(d[25])} logs having {len(d)} rows took {int((end - start) * 1000)}ms.")
            time.sleep(1)


class TestWellDatasetRedisAsyncTasks(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA

        self.f = '15_9-14.las'
        self.wellname = self.f.replace(".las", "")
        self.number_of_datasets = 200
        well = Well(self.wellname, new=True)
        dataset = WellDataset(well, "0")
        dataset.read_las(filename=os.path.join(self.path_to_test_data, f'{self.wellname}.las'), )
        data = dataset.get_data()
        for i in range(1, self.number_of_datasets):
            d = WellDataset(well, str(i))
            d.set_data({log: json.dumps(val) for log, val in data.items()})

    def test_async_normalization(self):

        logs = {"GR": {"min_value": 0, "max_value": 150, "output": "GR_norm"}, "RHOB": {"min_value": 1.5, "max_value": 2.5, "output": "RHOB_norm"}, }

        for i in range(self.number_of_datasets):
            async_normalize_log.delay(self.wellname, datasetname=str(i), logs=logs)
        # self.assertIn('one', well.datasets)

    def test_async_read_las(self):
        f = '7_1-2 S.las'
        wellname = f.replace(".las", "")
        well = Well(wellname, new=True)

        logs = {"FORCE_2020_LITHOFACIES_CONFIDENCE": 'float', "FORCE_2020_LITHOFACIES_LITHOLOGY": 'float', "CALI": 'float', "BS": 'float', "ROPA": 'float', "ROP": 'float',
                "RDEP": 'float',
                "RSHA": 'float', "RMED": 'float', "DTS": 'float', "DTC": 'float', "NPHI": 'float', "PEF": 'float', "GR": 'float', "RHOB": 'float', "DRHO": 'float',
                "DEPTH_MD": 'float', "X_LOC": 'float',
                "Y_LOC": 'float', "Z_LOC": 'float'}
        for i in range(50):
            async_read_las.delay(wellname, datasetname=str(i), filename=os.path.join('tests', self.path_to_test_data, f), logs=json.dumps(logs))
        print("Done")


if __name__ == '__main__':
    unittest.main()
