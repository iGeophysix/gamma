# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.
import filecmp
import math
import os
import unittest

from celery_conf import app as celery_app, check_task_completed
from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las
from components.importexport.las import import_to_db
from components.importexport.las.las_export import create_las_file
from settings import BASE_DIR


class TestLasImporter(unittest.TestCase):

    def test_las12(self):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'data/sample_minimal.las')
        las_file = las.parse(filename)

        self.assertEqual(las_file.version, 12, "Version should be 12")
        self.assertEqual(las_file.wrap, False, "Wrap shoulp be False")

        metrics = las_file.log_metrics_entries
        self.assertEqual(metrics['STRT'].value, '635.0000')
        self.assertEqual(metrics['STOP'].value, '400.0000')
        self.assertEqual(metrics['STEP'].value, '-0.1250')
        self.assertEqual(metrics['NULL'].value, '-999.25')

        req = las_file.required_well_entries
        self.assertEqual(req['COMP'].value, 'ANY OIL COMPANY INC.')
        self.assertEqual(req['WELL'].value, 'ANY ET AL A9-16-49-20')
        self.assertEqual(req['FLD'].value, 'EDAM')
        self.assertEqual(req['LOC'].value, 'A9-16-49-20W3M')
        self.assertEqual(req['PROV'].value, 'SASKATCHEWAN')
        self.assertEqual(req['SRVC'].value, 'ANY LOGGING COMPANY INC.')
        self.assertEqual(req['DATE'].value, '13-DEC-86')
        self.assertEqual(req['UWI'].value, '100091604920W300')

        curves = las_file.log_information_entries
        self.assertEqual(curves['DEPT'].units, 'M')
        self.assertEqual(curves['DEPT'].description, 'DEPTH')
        self.assertEqual(curves['ILM'].units, 'OHMM')
        self.assertEqual(curves['ILM'].description, 'MEDIUM RESISTIVITY')
        self.assertEqual(len(curves), 8)

        data = las_file.data
        self.assertEqual(data['DEPT'][1], float('634.8750'))

    def test_las20(self):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'data/sample_2.0_minimal.las')
        las_file = las.parse(filename)

        self.assertEqual(las_file.version, 20, "Version should be 20")
        self.assertEqual(las_file.wrap, False, "Wrap shoulp be False")

        metrics = las_file.log_metrics_entries
        self.assertEqual(metrics['STRT'].value, '635.0000')
        self.assertEqual(metrics['STOP'].value, '400.0000')
        self.assertEqual(metrics['STEP'].value, '-0.1250')
        self.assertEqual(metrics['NULL'].value, '-999.25')

        req = las_file.required_well_entries
        self.assertEqual(req['COMP'].value, 'ANY OIL COMPANY INC.')
        self.assertEqual(req['WELL'].value, 'ANY ET AL 12-34-12-34')
        self.assertEqual(req['FLD'].value, 'WILDCAT')
        self.assertEqual(req['LOC'].value, '12-34-12-34W5M')
        self.assertEqual(req['PROV'].value, 'ALBERTA')
        self.assertEqual(req['SRVC'].value, 'ANY LOGGING COMPANY INC.')
        self.assertEqual(req['DATE'].value, '13-DEC-86')
        self.assertEqual(req['UWI'].value, '100123401234W500')

    def test_las20_large(self):
        dirname = os.path.dirname(__file__)
        filename = os.path.join(dirname, 'data/sample_2.0_large.las')
        las_file = las.parse(filename)

        self.assertEqual(las_file.version, 20, "Version should be 20")
        self.assertEqual(las_file.wrap, False, "Wrap shoulp be False")

        metrics = las_file.log_metrics_entries
        self.assertEqual(metrics['STRT'].value, '30.0')
        self.assertEqual(metrics['STRT'].units, 'FT')
        self.assertEqual(metrics['STOP'].value, '594.0')
        self.assertEqual(metrics['STOP'].units, 'FT')
        self.assertEqual(metrics['STEP'].value, '0.1')
        self.assertEqual(metrics['NULL'].value, '-999.25')

        req = las_file.required_well_entries
        self.assertEqual(req['COMP'].value, 'KGS-OGS')
        self.assertEqual(req['WELL'].value, 'CURRENT # 1')
        self.assertEqual(req['FLD'].value, '')
        self.assertEqual(req['PROV'].value, '')
        self.assertEqual(req['CTRY'].value, 'US')
        self.assertEqual(req['STAT'].value, 'OKLAHOMA')
        self.assertEqual(req['CNTY'].value, 'PONTOTOC')
        self.assertEqual(req['SRVC'].value, '')
        self.assertEqual(req['DATE'].value, '06/10/2008')
        self.assertEqual(req['UWI'].value, '')

        add = las_file.additional_well_entries
        self.assertEqual(add['SEC'].value, '17')
        self.assertEqual(add['X'].value, '244102.82')
        self.assertEqual(add['X'].description, 'X or East-West coordinate')
        self.assertEqual(add['LATI'].value, 'N34.706')
        self.assertEqual(add['LATI'].description, 'Latitude')

        params = las_file.logging_parameters
        self.assertEqual(params['EGL'].value, '775')
        self.assertEqual(params['EGL'].units, 'M')
        self.assertEqual(params['EGL'].description, 'Ground Level Elevation')

        self.assertEqual(params['DFD'].value, '9.0')
        self.assertEqual(params['DFD'].units, 'gm/cc')
        self.assertEqual(params['DFD'].description, 'Mud Density')

        self.assertEqual(params['BHT'].value, '116.0')
        self.assertEqual(params['BHT'].units, 'DEG-F')
        self.assertEqual(params['BHT'].description, 'Maximum Recorded Temperature')

        self.assertEqual(params['ENG'].value, 'RUNNELS')
        self.assertEqual(params['ENG'].description, 'Recording Engineer')

        data = las_file.data
        self.assertEqual(data['POTASIUM'][4], float('2.77'))
        self.assertEqual(math.isnan(data['GAMMA'][5549]), False)
        self.assertEqual(math.isnan(data['GAMMA'][5550]), True)


class TestAsyncLasLoading(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

    def test_celery_las_loading(self):
        folder = os.path.join(os.path.dirname(__file__), 'data')
        paths = list(map(lambda x: os.path.join(folder, x), os.listdir(folder)))

        async_results = []
        for path in paths:
            if not path.endswith('las'):
                continue
            with open(path, 'rb') as f:
                data = f.read()
            async_results.append(celery_app.send_task('tasks.async_read_las', kwargs={'filename': path, 'las_data': data}))

        while not all(map(check_task_completed, async_results)):
            continue

        p = Project()
        wells = p.list_wells()
        self.assertEqual(3, len(wells), "3 wells must be loaded")
        datasets = []
        for well in wells:
            datasets.extend(Well(well).datasets)
        self.assertEqual(3, len(datasets), "3 datasets must be loaded")

        w = Well('ANY ET AL A9-16-49-20')
        ds = WellDataset(w, 'sample_minimal.las')
        logs = ds.log_list
        self.assertEqual(7, len(logs), '7 logs in dataset must be loaded')


def wait_till_completed(async_results):
    pass


class TestLasExport(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        self.folder = os.path.join(BASE_DIR, 'components', 'importexport', 'test', 'data')
        well = Well('w1', new=True)
        ds = WellDataset(well, 'ds1', new=True)
        import_to_db(filename=os.path.join(self.folder, 'sample_2.0_large.las'), well=well, well_dataset=ds)

    def test_las_export(self):
        well_name = 'w1'
        paths_to_logs = (
            ('ds1', 'GAMMA'),
            ('ds1', 'URANIUM'),
            ('ds1', 'THORIUM'),
            ('ds1', 'POTASIUM'),
        )
        las = create_las_file(well_name, paths_to_logs)
        las.well.DATE = '2021-05-13 11:37:04'  # fixture for tests only
        las.write(os.path.join(self.folder, 'exported.las'), version=2)
        self.assertTrue(filecmp.cmp(os.path.join(self.folder, 'true_exported.las'), os.path.join(self.folder, 'exported.las')))


if __name__ == '__main__':
    unittest.main()
