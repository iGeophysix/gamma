# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.
import math
import os
import unittest

from celery_conf import app as celery_app, wait_till_completes
from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las


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
        paths = ['sample_2.0_large.las',
                 'sample_2.0_minimal.las',
                 'sample_minimal.las']
        paths = list(map(lambda x: os.path.join(folder, x), paths))

        async_results = []
        for path in paths:
            if not path.endswith('las'):
                continue
            with open(path, 'rb') as f:
                data = f.read()
            async_results.append(celery_app.send_task('tasks.async_read_las', kwargs={'filename': path, 'las_data': data}))

        wait_till_completes(async_results)

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
        self.assertEqual(8, len(logs), '8 logs in dataset must be loaded')


if __name__ == '__main__':
    unittest.main()
