import os
from unittest import TestCase

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.importexport.well_heads import import_well_heads_csv, well_heads_csv_header
from settings import BASE_DIR


class TestWellHeadsImport(TestCase):
    def setUp(self):
        s = RedisStorage()
        s.flush_db()

    def test_import_well_heads_csv(self):
        '''
        Import well heads with units and check it is retrievable
        :return:
        '''
        with open(os.path.join(BASE_DIR, 'test_data', 'wellheads', 'Smtlr_WellCoodinates_Example.csv'), 'r') as f:
            header = well_heads_csv_header(f, delimiter=';')
            import_well_heads_csv(f, header, delimiter=';')

        well = Well('4735')
        self.assertEqual(13514.02, well.info['X']['value'])
        self.assertEqual('m', well.info['X']['units'])

    def test_import_well_heads_csv_no_units(self):
        """
        Import well heads WITHOUT units and check it is retrievable
        :return:
        """
        with open(os.path.join(BASE_DIR, 'test_data', 'wellheads', 'Smtlr_WellCoodinates_No_Units.csv'), 'r') as f:
            header = well_heads_csv_header(f, units_row=None, delimiter=';')
            import_well_heads_csv(f, header, delimiter=';')
            well = Well('4735')
            self.assertEqual(13514.02, well.info['X']['value'])
            self.assertEqual('', well.info['X']['units'])