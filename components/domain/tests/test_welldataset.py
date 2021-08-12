import os
import unittest

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
        self.assertTrue(dataset.exists)

    def test_change_dataset_info(self):
        well = Well('well2', new=True)
        dataset = WellDataset(well, "one", new=True)
        info = dataset.meta
        info['extra_data'] = 'toto'
        dataset.meta = info
        self.assertEqual(dataset.meta['extra_data'], 'toto')

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

    def test_check_las_header(self):
        f = 'another_small_file.las'

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f))
        true_info = {
            'WELL': {
                'value': '15/9-13 Sleipner East Appr',
                'description': 'WELL'
            },
            'COMP': {
                'value': '',
                'description': 'COMPANY'
            },
            'SRVC': {
                'value': '',
                'description': 'SERVICE COMPANY'
            },
            'FLD': {
                'value': '',
                'description': 'FIELD'
            },
            'LOC': {
                'value': '',
                'description': 'LOCATION'
            },
            'DATE': {
                'value': '2020-08-09 20:01:10   : Log Export Date {yyyy-MM-dd HH:mm',
                'description': 'ss}'
            },
            'CTRY': {
                'value': '',
                'description': ''
            },
            'STAT': {
                'value': '',
                'description': ''
            },
            'CNTY': {
                'value': '',
                'description': ''
            },
            'PROV': {
                'value': '',
                'description': 'PROVINCE'
            },
            'API': {
                'value': '',
                'description': 'API NUMBER'
            },
            'UWI': {
                'value': '15/9-13',
                'description': 'UNIQUE WELL ID'
            }
        }
        wellname = "15/9-13 Sleipner East Appr"
        well = Well(wellname)
        self.assertEqual(true_info, well.meta)
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
        dataset.meta = true_info
        self.assertEqual(true_info, dataset.meta)
        well.delete()

    def test_logs_list(self):
        f = 'small_file.las'
        wellname = f[:-4]
        well = Well(wellname, new=True)
        dataset = WellDataset(well, "one", new=True)

        las.import_to_db(filename=os.path.join(self.path_to_test_data, f),
                         well=well,
                         well_dataset=dataset)

        log_list = dataset.get_log_list()
        self.assertNotIn("BOBOB", log_list)
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
            log.meta.update(new_meta)
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