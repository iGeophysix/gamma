import os
import string
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


class TestLog(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.well = Well("test", new=True)
        self.dataset = WellDataset(self.well, "test", new=True)
        self.path_to_test_data = PATH_TO_TEST_DATA

    def test_create_two_logs(self):

        data = {"GR": np.array(((10, 1), (20, 2))),
                "PS": np.array(((10, 3), (20, 4)))}
        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "uV", "code": "", "description": "PS"}}

        log1 = BasicLog(self.dataset.id, name="GR")
        log1.values = data["GR"]
        log1.meta = meta["GR"]
        log1.save()

        log2 = BasicLog(self.dataset.id, name="PS")
        log2.values = data["PS"]
        log2.meta = meta["PS"]
        log2.save()

        received_data = self.dataset.get_log_data()
        self.assertTrue(np.isclose(received_data["GR"].values, data["GR"], equal_nan=True).all())
        self.assertTrue(np.isclose(received_data["PS"].values, data["PS"], equal_nan=True).all())

        self.assertEqual(self.dataset.get_log_meta(), meta)

    def test_log_get_data(self):
        f = 'small_file.las'
        ref_depth = 200.14440000
        wellname = '15_9-13'
        dataset_name = 'one'
        well = Well(wellname, new=True)
        dataset = WellDataset(well, dataset_name, new=True)

        dataset.info = las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                                        well=well,
                                        well_dataset=dataset)

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

        d = dataset.get_log_data()

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

        for log_name, new_meta in extra_meta.items():
            log = BasicLog(dataset.id, log_name)
            log.meta = log.meta | new_meta

            log.save()

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

        log = BasicLog(dataset.id, "GR")
        self.assertEqual(f'Loaded from {f}', log.history[0][1])

    def test_append_log_meta(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)

        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "uV", "code": "", "description": "PS"}}

        for log_name, new_meta in meta.items():
            log = BasicLog(dataset.id, log_name)
            log.meta = new_meta
            log.save()

        log = BasicLog(dataset.id, "GR")
        log.meta = log.meta | {"max_depth": 100}
        log.save()

        self.assertEqual(log.meta['max_depth'], 100)
