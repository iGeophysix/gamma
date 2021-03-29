import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las import import_to_db
from tasks import async_split_by_runs, async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestSplitByRun(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA
        wellname = '622'
        well = Well(wellname, True)
        for file in ['Well622_ULN_Combined.las', ]:
            f = os.path.join(self.path_to_test_data, file)
            dataset_name = file[:-4]
            dataset = WellDataset(well, dataset_name, True)
            import_to_db(f, well=well, well_dataset=dataset)

    def test_split_by_runs(self):
        wellname = '622'
        datasets = ['Well622_ULN_Combined', ]
        async_get_basic_log_stats(wellname, datasets)
        async_split_by_runs(wellname, datasets, 50)

        # export results to csv
        w = Well(wellname)

        d = WellDataset(w, 'Well622_ULN_Combined')
        t = d.get_log_meta(['GK$_D2711_D', ])
        self.assertEqual('2_(2656.8_2720.0)', t['GK$_D2711_D']['Run_AutoCalculated'], )
