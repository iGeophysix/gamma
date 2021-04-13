import os
import string
import time
import unittest
from datetime import datetime, timedelta
from random import randint, random, choice

import numpy as np

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestWellDatasetRedis(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_one_dataset(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)
        data = {"GR": np.array(((10, 1), (20, 2))),
                "PS": np.array(((10, 3), (20, 4)))}
        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "uV", "code": "", "description": "PS"}}
        dataset.set_data(data, meta)

        received_data = dataset.get_log_data()
        self.assertTrue(np.isclose(received_data["GR"].values, data["GR"], equal_nan=True).all())
        self.assertTrue(np.isclose(received_data["PS"].values, data["PS"], equal_nan=True).all())

        self.assertEqual(dataset.get_log_meta(), meta)

    def test_change_dataset_info(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)
        info = dataset.info
        info['extra_data'] = 'toto'
        dataset.info = info
        self.assertEqual(dataset.info['extra_data'], 'toto')

    def test_create_and_delete_datasets(self):
        f = 'small_file.las'
        wellname = f.replace(".las", "")

        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        self.assertIn('one', well.datasets)

        dataset = WellDataset(well, "two", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        dataset = WellDataset(well, "one")
        dataset.delete()
        self.assertNotIn('one', well.datasets)
        self.assertIn('two', well.datasets)

    def test_dataset_get_data(self):
        f = 'small_file.las'
        ref_depth = 200.14440000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name, new=True)

        well_info = las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                                     well=well,
                                     well_dataset=dataset)

        well.info = well_info  # TODO: Should it be assigned to well or to dataset?

        data = dataset.get_log_data(start=ref_depth - 0.001, end=ref_depth + 0.001)
        true_answer = {'DRHO': np.nan,
                       'NPHI': np.nan,
                       'FORCE_2020_LITHOFACIES_CONFIDENCE': np.nan,
                       'PEF': np.nan,
                       'FORCE_2020_LITHOFACIES_LITHOLOGY': np.nan,
                       'CALI': np.nan,
                       'y_loc': 6421723.0,
                       'ROP': np.nan,
                       'RSHA': 1.4654846191,
                       'DTC': np.nan,
                       'RDEP': 1.0439596176,
                       'RHOB': np.nan,
                       'DEPTH_MD': 200.14439392,
                       'BS': 17.5,
                       'DTS': np.nan,
                       'ROPA': np.nan,
                       'GR': 9.0210666656,
                       'RMED': 1.7675967216,
                       'z_loc': -156.1439972,
                       'x_loc': 444904.03125}

        for key in true_answer.keys():
            value = data[key][0, 1]  # [row, column]
            self.assertTrue(np.isclose(value, true_answer[key], equal_nan=True))

    def test_check_las_header(self):
        f = 'another_small_file.las'

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f))

        true_info = {
            "WELL": [
                "15/9-13 Sleipner East Appr",
                "WELL"
            ],
            "COMP": [
                "",
                "COMPANY"
            ],
            "SRVC": [
                "",
                "SERVICE COMPANY"
            ],
            "FLD": [
                "",
                "FIELD"
            ],
            "LOC": [
                "",
                "LOCATION"
            ],
            "DATE": [
                "2020-08-09 20:01:10   : Log Export Date {yyyy-MM-dd HH:mm",
                "ss}"
            ],
            "CTRY": [
                "",
                ""
            ],
            "STAT": [
                "",
                ""
            ],
            "CNTY": [
                "",
                ""
            ],
            "PROV": [
                "",
                "PROVINCE"
            ],
            "API": [
                "",
                "API NUMBER"
            ],
            "UWI": [
                "15/9-13",
                "UNIQUE WELL ID"
            ]
        }

        wellname = "15/9-13 Sleipner East Appr"
        well = Well(wellname)

        self.assertEqual(true_info, well.info)
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

    def test_add_many_logs(self):
        f = 'small_file.las'
        log_count = 5
        LOG_TYPES = (float, str, int, bool, datetime,)

        wellname = 'thousand_logs'
        datasetname = 'this_dataset'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, datasetname, new=True)

        # load some real data
        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        # create logs in the dataset
        new_logs = {f"LOG_{i}": LOG_TYPES[randint(0, len(LOG_TYPES) - 1)] for i in range(0, log_count)}
        new_logs_meta = {f"LOG_{i}": {"units": "some_units", "code": i, "description": f"Dummy log {i}"} for i in range(0, log_count)}
        # get depths
        arr = dataset.get_log_data(logs=["GR", ])["GR"]
        existing_depths = arr[:, 0]

        # add data to the logs
        def dummy_data(dtype):  # return scalar
            generators = {
                float: 400 * random() - 200,
                str: ''.join(choice(string.ascii_letters) for _ in range(64)),
                int: randint(-1000, 1000),
                datetime: datetime.strftime(datetime.now() + random() * timedelta(days=1), "%Y-%m-%d %H:%M:%S.%f%z"),
                bool: 1 == randint(0, 1)
            }
            return generators[dtype]

        def dummy_log(depths, dtype):
            return np.array([(depth, dummy_data(dtype)) for depth in depths])

        for new_log, log_type in new_logs.items():
            log = BasicLog(dataset_id=dataset.id, name=new_log)
            log.values = dummy_log(existing_depths, log_type)
            log.meta = new_logs_meta[new_log]
            log.save()

        start = time.time()
        d = dataset.get_log_data()

        end = time.time()
        print(f"Read of {len(d)} logs having {len(d['GR'])} rows took {int((end - start) * 1000)}ms.")

        self.assertEqual(len(d), 20 + log_count)
        self.assertEqual(len(d['LOG_1']), 84)

    def test_logs_list(self):
        f = 'small_file.las'
        wellname = f[:-4]
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        log_list = dataset.get_log_list()
        self.assertNotIn("DEPT", log_list)
        self.assertIn("GR", log_list)

    def test_logs_list_specify_meta(self):
        f = 'small_file.las'
        wellname = f[:-4]
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        extra_meta = {"GR": {"mean": 5},
                      "DTS": {"mean": 1},
                      "RDEP": {"mean": 10},
                      "NPHI": {"mean": 100},
                      }
        dataset.append_log_meta(extra_meta)

        log_list = dataset.get_log_list(description='RSHA')
        self.assertNotIn("GR", log_list)
        self.assertIn("RSHA", log_list)

        log_list = dataset.get_log_list(mean__lt=10)
        self.assertListEqual(['DTS', 'GR'], sorted(log_list))

        log_list = dataset.get_log_list(mean__gt=10)
        self.assertListEqual(['NPHI', ], log_list)

        log_list = dataset.get_log_list(mean__gt=3, mean__lt=70)
        self.assertListEqual(['GR', 'RDEP'], sorted(log_list))

        log_list = dataset.get_log_list(mean__gt=3, mean__lt=70, description='GR')
        self.assertListEqual(['GR', ], log_list)

    def test_log_history(self):
        f = 'small_file.las'
        wellname = "log_history_test"
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        history = dataset.get_log_history("GR")
        self.assertEqual(f'Loaded from {f}', history[0][1])

    def test_append_log_meta(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)
        data = {"GR": np.array(((10, 1), (20, 2))),
                "PS": np.array(((10, 3), (20, 4)))}
        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "uV", "code": "", "description": "PS"}}
        dataset.set_data(data, meta)

        dataset.append_log_meta(meta={"GR": {"max_depth": 100}})

        self.assertEqual(dataset.get_log_meta()['GR']['max_depth'], 100)
