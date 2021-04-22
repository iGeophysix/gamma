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
        true_meta = {"GR": {'__history': [],
                            '__type': 'BasicLog',
                            'code': '',
                            'description': 'GR',
                            'units': 'gAPI'},
                     "PS": {'__history': [],
                            '__type': 'BasicLog',
                            'code': '',
                            'description': 'PS',
                            'units': 'uV'},
                     }
        log1 = BasicLog(self.dataset.id, id="GR")
        log1.values = data["GR"]
        log1.meta = meta["GR"]
        log1.save()

        log2 = BasicLog(self.dataset.id, id="PS")
        log2.values = data["PS"]
        log2.meta = meta["PS"]
        log2.save()

        for log_name in data.keys():
            log = BasicLog(self.dataset.id, log_name)
            self.assertTrue(np.isclose(log.values, data[log_name], equal_nan=True).all())
            self.assertEqual(true_meta[log_name], log.meta)

    def test_name_works_correctly(self):
        log_id = 'GRTRTT'
        gr = BasicLog(id=log_id)
        self.assertEqual(log_id, gr.name)
        gr.name = 'GR'
        self.assertEqual('GR', gr.name)
        self.assertFalse(gr.exists())
        gr.dataset_id = self.dataset.id
        gr.save()
        self.assertTrue(gr.exists())

        gr1 = BasicLog(dataset_id=self.dataset.id, id=log_id)
        self.assertTrue(gr1.exists())
        self.assertEqual("GR", gr1.name)

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

        for log_name in true_answer.keys():
            log = BasicLog(dataset.id, log_name)
            log.crop(depth=ref_depth, inplace=True)
            value = log[0, 1]  # [row, column]
            self.assertTrue(np.isclose(value, true_answer[log_name], equal_nan=True))

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
        new_logs = {f"LOG_{i}": log_type for i, log_type in enumerate(LOG_TYPES)}
        new_logs_meta = {f"LOG_{i}": {"units": "some_units", "code": i, "description": f"Dummy log {i}"} for i in range(0, log_count)}
        # get depths
        existing_depths = BasicLog(dataset.id, "GR").values[:, 0]

        # add data to the log_names
        def dummy_data(dtype):  # returns scalar
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
            log = BasicLog(dataset_id=dataset.id, id=new_log)
            log.values = dummy_log(existing_depths, log_type)
            log.meta = new_logs_meta[new_log]
            log.save()

        self.assertEqual(len(dataset.log_list), 20 + log_count)
        d = BasicLog(dataset.id, 'LOG_1')
        self.assertEqual(len(d), 84)

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
            log.meta |=  new_meta

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
        log.meta |=  {"max_depth": 100}
        log.save()

        self.assertEqual(log.meta['max_depth'], 100)

    def test_unit_conversion_works_correctly(self):
        well = Well('unit_conversion')
        dataset = WellDataset(well, "test")
        welllog = BasicLog(dataset.id, "log")
        welllog.name = "log"
        welllog.units = "cm"
        welllog.values = np.array([(10, 10), (20, 20)])

        vals_in_m = welllog.convert_units('km')
        self.assertListEqual([10 ** -4, 2 * 10 ** -4], list(vals_in_m[:, 1]))

        welllog.units = "kg"
        vals_in_m = welllog.convert_units('g')
        self.assertListEqual([10000, 20000], list(vals_in_m[:, 1]))

    def test_adding_removing_tags(self):
        well = Well('tags')
        dataset = WellDataset(well, "test")
        welllog = BasicLog(dataset.id, "log")
        # no tags in the log - empty list
        self.assertEqual(set(), welllog.tags)
        self.assertEqual(set, type(welllog.tags))
        # add one tag - and check it is there
        welllog.append_tags("tag1")
        self.assertCountEqual({"tag1"}, welllog.tags)
        self.assertEqual(set, type(welllog.tags))
        # do not add duplicated tag
        welllog.append_tags("tag1")
        self.assertCountEqual({"tag1"}, welllog.tags)
        # add multiple new logs
        welllog.append_tags("tag1", "tag2", "tag3")
        self.assertCountEqual({"tag1", "tag2", "tag3"}, welllog.tags)
        # delete one tag
        welllog.delete_tags("tag1")
        self.assertCountEqual({"tag2", "tag3"}, welllog.tags)
        # check cannot delete missing tag
        with self.assertRaises(KeyError):
            welllog.delete_tags("tag1")
        # check delete multiple tags
        welllog.delete_tags("tag2", "tag3")
        self.assertEqual(set(), welllog.tags)

    def test_hashing_works_correctly(self):
        f = 'small_file.las'
        wellname = f[:-4]
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        log = BasicLog(dataset.id, "GR")
        log.update_hashes()
        log.save()

        true_values = {
            "data_hash": "43b6e247100f1688023b4e915ad852d0",
            "meta_hash": "0c16899f5e0772549cadd18ee472368a",
            "full_hash": "43b6e247100f1688023b4e915ad852d00c16899f5e0772549cadd18ee472368a"
        }
        test_log = BasicLog(dataset.id, "GR")
        self.assertEqual(true_values['data_hash'], test_log.data_hash)
        self.assertEqual(true_values['meta_hash'], test_log.meta_hash)
        self.assertEqual(true_values['full_hash'], test_log.full_hash)
