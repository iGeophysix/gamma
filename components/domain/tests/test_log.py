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
from components.importexport.markers_importexport import import_markers_csv
from settings import BASE_DIR


class TestLog(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.well = Well("test", new=True)
        self.dataset = WellDataset(self.well, "test", new=True)
        self.path_to_test_data = os.path.join(BASE_DIR, 'components', 'domain', 'tests', 'test_data')

    def test_create_two_logs(self):

        data = {"GR": np.array(((10, 1.0), (20, 2.0))),
                "PS": np.array(((10, 3.0), (20, 4.0)))}
        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "mV", "code": "", "description": "PS"}}
        log1 = BasicLog(self.dataset.id, log_id="GR")
        log1.values = data["GR"]
        log1.meta = meta["GR"]
        log1.save()

        log2 = BasicLog(self.dataset.id, log_id="PS")
        log2.values = data["PS"]
        log2.meta = meta["PS"]
        log2.save()

        for log_name in data.keys():
            log = BasicLog(self.dataset.id, log_name)
            self.assertTrue(np.isclose(log.values, data[log_name], equal_nan=True).all())

    def test_log_meta_parsed_to_properties(self):
        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"}, }

        log1 = BasicLog(self.dataset.id, log_id="GR")
        log1.meta = meta["GR"]
        log1.meta.one_more_field = "test_value"
        log1.save()

        self.assertEqual('GR', log1.meta.name)
        self.assertEqual([], log1.meta.tags)
        self.assertEqual('test_value', log1.meta.one_more_field)

    def test_name_works_correctly(self):
        log_id = 'GRTRTT'
        gr = BasicLog(log_id=log_id)
        self.assertEqual(log_id, gr.name)
        gr.meta.name = 'GR'
        self.assertEqual('GR', gr.name)
        self.assertFalse(gr.exists())
        gr.meta.dataset_id = self.dataset.id
        gr.save()
        self.assertTrue(gr.exists())

        gr1 = BasicLog(dataset_id=self.dataset.id, log_id=log_id)
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
            log = BasicLog(dataset_id=dataset.id, log_id=new_log)
            log.values = dummy_log(existing_depths, log_type)
            log.meta = new_logs_meta[new_log]
            log.save()

        self.assertEqual(len(dataset.log_list), 21 + log_count)
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
        self.assertIn("GR", log_list)

    def test_log_history(self):
        f = 'small_file.las'
        wellname = "log_history_test"
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        log = BasicLog(dataset.id, "GR")
        self.assertEqual(f'Loaded from {f}', log.meta.history[0][1])

    def test_append_log_meta(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)

        meta = {"GR": {"units": "gAPI", "code": "", "description": "GR"},
                "PS": {"units": "mV", "code": "", "description": "PS"}}

        for log_name, new_meta in meta.items():
            log = BasicLog(dataset.id, log_name)
            log.meta = new_meta
            log.save()

        log = BasicLog(dataset.id, "GR")
        log.meta.max_depth = 100
        log.save()

        self.assertEqual(log.meta.max_depth, 100)

    def test_unit_conversion_works_correctly(self):
        well = Well('unit_conversion')
        dataset = WellDataset(well, "test")
        welllog = BasicLog(dataset.id, "log")
        welllog.name = "log"
        welllog.meta.units = "cm"
        welllog.values = np.array([(10, 10), (20, 20)])

        vals_in_m = welllog.convert_units('km')
        self.assertListEqual([10 ** -4, 2 * 10 ** -4], list(vals_in_m[:, 1]))

        welllog.meta.units = "kg"
        vals_in_m = welllog.convert_units('g')
        self.assertListEqual([10000, 20000], list(vals_in_m[:, 1]))

    def test_adding_removing_tags(self):
        well = Well('tags')
        dataset = WellDataset(well, "test")
        welllog = BasicLog(dataset.id, "log")
        # no tags in the log - empty list
        self.assertEqual(list(), welllog.meta.tags)
        self.assertEqual(list, type(welllog.meta.tags))
        # add one tag - and check it is there
        welllog.meta.add_tags("tag1", )
        welllog.save()
        self.assertCountEqual({"tag1"}, welllog.meta.tags)
        self.assertEqual(list, type(welllog.meta.tags))
        # do not add duplicated tag
        welllog.meta.add_tags("tag1")
        self.assertCountEqual(["tag1"], welllog.meta.tags)
        # add multiple new logs
        welllog.meta.add_tags("tag1", "tag2", "tag3")
        self.assertCountEqual(["tag1", "tag2", "tag3"], welllog.meta.tags)
        # delete one tag
        welllog.meta.delete_tags("tag1")
        welllog.save()
        self.assertCountEqual(["tag2", "tag3"], welllog.meta.tags)
        # check cannot delete missing tag
        with self.assertRaises(ValueError):
            welllog.meta.delete_tags("tag1")
        # check delete multiple tags
        welllog.meta.delete_tags("tag2", "tag3")
        welllog.save()
        self.assertEqual([], welllog.meta.tags)
        dataset.delete_log(welllog.name)

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
            "meta_hash": "a9bbbe27c7043ab26b44dde3b7e7af9c",
            "full_hash": "43b6e247100f1688023b4e915ad852d0a9bbbe27c7043ab26b44dde3b7e7af9c"
        }
        test_log = BasicLog(dataset.id, "GR")
        self.assertEqual(true_values['data_hash'], test_log.data_hash)
        self.assertEqual(true_values['meta_hash'], test_log.meta_hash)
        self.assertEqual(true_values['full_hash'], test_log.full_hash)


class TestMarkersLog(unittest.TestCase):
    def setUp(self) -> None:
        pass
        # _s = RedisStorage()
        # _s.flush_db()
        self.path_to_test_data = os.path.join(BASE_DIR, 'test_data')
        # for filename in os.listdir(os.path.join(self.path_to_test_data, 'ProjectData')):
        #     if not filename.endswith('.las'):
        #         continue
        #     import_to_db(filename=os.path.join(self.path_to_test_data, 'ProjectData', filename))

    def test_markers_with_gaps_loading_from_csv(self):
        with open(os.path.join(self.path_to_test_data, 'Markers', 'FieldData_StratigraphyWithoutGaps.csv')) as raw_data:
            import_markers_csv(raw_data=raw_data, missing_value='-9999')
