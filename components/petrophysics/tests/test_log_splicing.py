import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.log_splicing import SpliceLogsNode
from tasks import async_get_basic_log_stats, async_read_las, async_log_resolution, async_splice_logs

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestLogSplicing(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        self.wellname = '609'
        self.w = Well(self.wellname, new=True)

        # loading data
        filename = '609_ULN_LQC.las'
        self.wd = WellDataset(self.w, filename, new=True)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=os.path.join(PATH_TO_TEST_DATA, filename))

        # calculating basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

        # adding more metadata
        meta = {
            'GK_D2258_D': {'family': 'Gamma Ray', 'run': {'value': '70_(2450_2600)'}, 'tags': ['processing', ]},
            'GK_D1910_D': {'family': 'Gamma Ray', 'run': {'value': '60_(2450_2600)'}, 'tags': ['processing', ]},
            'GK_D1911_D_2': {'family': 'Gamma Ray', 'run': {'value': '50_(2450_2600)'}, 'tags': ['processing', ]},
            'GK_D2395_D': {'family': 'Gamma Ray', 'run': {'value': '40_(2450_2600)'}, 'tags': ['processing', ]},
            'GK_D2265_D': {'family': 'Gamma Ray', 'run': {'value': '30_(2450_2600)'}, 'tags': ['processing', ]},
            'GK_D1911_D': {'family': 'Gamma Ray', 'run': {'value': '20_(2450_2600)'}, 'tags': ['processing', ]},
        }
        for log_id, values in meta.items():
            l = BasicLog(self.wd.id, log_id)
            l.meta = values
            l.save()

        # define log resolution
        async_log_resolution(self.w.name, datasetnames=[filename, ])

    def test_log_splicing_works_correctly(self):
        async_splice_logs(wellname='609', output_dataset_name='Spliced')
        wd = WellDataset(self.w, 'Spliced')
        log = BasicLog(wd.id, 'Gamma Ray')
        true_meta = {'AutoSpliced': {'Intervals': 6, 'Uncertainty': 0.5},
                     'family': 'Gamma Ray',
                     'basic_statistics': {
                         'avg_step': 0.09999999999999964,
                         'const_step': True,
                         'depth_span': 2634.6999999999907,
                         'gmean': 3.8136451008782117,
                         'max_depth': 2643.2999999999906,
                         'max_value': 8.497804999995498,
                         'mean': 4.031003762634724,
                         'min_depth': 8.6,
                         'min_value': 0.9,
                         'stdev': 1.2796598690478778}
                     }
        for key, val in true_meta.items():
            self.assertEqual(val, log.meta[key])

    def test_log_splicing_engine_node_works_correctly(self):
        node = SpliceLogsNode()
        node.run(output_dataset_name='LQC2')

        wd = WellDataset(self.w, 'LQC2')
        log = BasicLog(wd.id, 'Gamma Ray')
        true_meta = {'AutoSpliced': {'Intervals': 6, 'Uncertainty': 0.5},
                     'family': 'Gamma Ray',
                     'basic_statistics': {
                         'avg_step': 0.09999999999999964,
                         'const_step': True,
                         'depth_span': 2634.6999999999907,
                         'gmean': 3.8136451008782117,
                         'max_depth': 2643.2999999999906,
                         'max_value': 8.497804999995498,
                         'mean': 4.031003762634724,
                         'min_depth': 8.6,
                         'min_value': 0.9,
                         'stdev': 1.2796598690478778},
                     'tags': ['spliced']
                     }
        for key, val in true_meta.items():
            self.assertEqual(val, log.meta[key])
