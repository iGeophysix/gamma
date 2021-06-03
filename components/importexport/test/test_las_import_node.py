import os
import unittest

import botocore

from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las_importexport import LasImportNode, LasExportNode
from settings import BASE_DIR
from tasks import async_read_las, async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'components', 'importexport', 'test', 'data')


class TestLasImporter(unittest.TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

    def test_las_import_works(self):
        folder = os.path.join(os.path.dirname(__file__), 'data')
        paths = ['sample_2.0_large.las',
                 'sample_2.0_minimal.las',
                 'sample_minimal.las']
        paths = list(map(lambda x: os.path.join(folder, x), paths))
        node = LasImportNode()
        node.run([p for p in paths if p.endswith('.las')])

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


class TestLasExportNode(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = '15-9-19'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'sample_2.0_large.las'
        self.wd = WellDataset(self.w, 'LQC', new=True)
        self.output_wd = WellDataset(self.w, 'LQC', new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname='LQC', filename=test_data)
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=['LQC', ])

    def test_export_node_works_correctly(self):
        node = LasExportNode()
        node.run(destination='TestExport')

        s3 = LasExportNode.s3

        try:
            s3.download_file('public', 'TestExport/15-9-19_LQC.las', 'deleteme.las')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                self.fail("File not found on S3")
            else:
                # Something else has gone wrong.
                raise
        os.remove('deleteme.las')

    def test_export_well_method_raises_exception(self):
        node = LasExportNode()
        with self.assertRaises(KeyError):
            node.export_well_dataset('TestExport', '15-9-19', datasetname='LQC', logs=['GR', 'TNPH'])
