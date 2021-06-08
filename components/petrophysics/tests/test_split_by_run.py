import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las import import_to_db
from components.petrophysics.run_detection import RunDetectionNode
from settings import BASE_DIR
from tasks import async_split_by_runs, async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


class TestSplitByRun(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        self.path_to_test_data = PATH_TO_TEST_DATA
        wellname = '622'
        well = Well(wellname, True)
        datasets = ['Well622_ULN_Combined', ]
        for file in datasets:
            f = os.path.join(self.path_to_test_data, file + '.las')
            dataset_name = file
            dataset = WellDataset(well, dataset_name, True)
            import_to_db(f, well=well, well_dataset=dataset)
        async_get_basic_log_stats(wellname, datasets)

    def test_split_by_runs(self):
        wellname = '622'
        async_split_by_runs(wellname, depth_tolerance=50)

        w = Well(wellname)
        d = WellDataset(w, 'Well622_ULN_Combined')
        l = BasicLog(d.id, 'GK$_D2711_D')
        self.assertEqual(l.meta.run['value'], '13_(2657.1-2720.3)')

    def test_split_by_runs_engine_node(self):
        node = RunDetectionNode()
        node.run()

        w = Well('622')
        d = WellDataset(w, 'Well622_ULN_Combined')
        l = BasicLog(d.id, 'GK$_D2711_D')
        self.assertEqual(l.meta.run['value'], '13_(2657.1-2720.3)')
