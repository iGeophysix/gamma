import os
import unittest

from database.RedisStorage import RedisStorage
from domain.Well import Well
from domain.WellDataset import WellDataset
from tasks import async_split_by_runs

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestBestLogSelection(unittest.TestCase):
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
            dataset.read_las(os.path.join(self.path_to_test_data, f))

    def test_split_by_runs(self):
        wellname = '622'
        datasets = ['Well622_ULN_Combined.las', ]
        async_split_by_runs(wellname, datasets)

        # export results to csv
        w = Well(wellname)

        # uncomment this to export data to csv
        '''
        run_info = []
        for dataset in w.datasets:
            d = WellDataset(w, dataset)
            log_meta = d.get_log_meta()
            run_info.extend([{'well': wellname,
                              'dataset': dataset,
                              'mnemonic': l,
                              'run': v['RUN'],
                              'min_depth': v['min_depth'],
                              'max_depth': v['max_depth'],
                              'mean': v['mean'],
                              'gmean': v['mean'],
                              'stdev': v['stdev']} for l, v in log_meta.items()]
                            )
        pd.DataFrame(run_info).to_csv('run_split.csv', index=False)
        '''

        d = WellDataset(w, 'Well622_ULN_Combined')
        t = d.get_log_meta(['GK$_D2711_D',])
        self.assertEqual(t['GK$_D2711_D']['RUN'], '3_(2656.8_2720.0)')
