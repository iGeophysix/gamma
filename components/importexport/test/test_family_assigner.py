import os.path
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineProgress
from components.importexport.FamilyAssigner import FamilyAssigner, FamilyAssignerNode
from components.importexport.las import import_to_db


class TestFamilyAssignment(unittest.TestCase):
    def setUp(self):
        self.fa = FamilyAssigner()
        self.log_list = [('GR.NORM', 'GAPI'),
                         ('PS', 'mV'),
                         ('RB', 'deg'),
                         ('BS', 'mm'),
                         ('RAW-TNPH', '%'),
                         ('FCAZ', None),
                         ('HAZI', None),
                         ('CALI 2', None),
                         ('ГЗ1', None),
                         ('СП', None),
                         ('ПС', None),
                         ('RHOC_1', 'g/cm3'),
                         ('RHOC_2', 'ohm.m'),
                         ('RT_DK', 'ohmm')]

    def test_one_by_one(self):
        result = ['Gamma Ray',
                  'Spontaneous Potential',
                  'Relative Bearing',
                  'Nom Borehole Diameter',
                  'Neutron Porosity',
                  'Z Acceleration',
                  'Borehole Azimuth',
                  'Caliper',
                  'Resistivity',
                  'Spontaneous Potential',
                  'Spontaneous Potential',
                  'Bulk Density Correction',
                  'Resistivity',
                  'Formation Resistivity']

        for n, mnemonic_unit in enumerate(self.log_list):
            mnemonic, unit = mnemonic_unit
            res = self.fa.assign_family(mnemonic, unit, one_best=True)
            self.assertIsNotNone(res)
            self.assertEqual(res.family, result[n], f'{mnemonic} failed')

    def test_batch(self):
        result = ['Gamma Ray',
                  'Spontaneous Potential',
                  'Relative Bearing',
                  'Nom Borehole Diameter',
                  'Neutron Porosity',
                  'Z Acceleration',
                  'Borehole Azimuth',
                  'Caliper',
                  'Resistivity',
                  'Spontaneous Potential',
                  'Spontaneous Potential',
                  'Bulk Density Correction',
                  'Resistivity',
                  'Formation Resistivity']

        res = self.fa.assign_families(self.log_list)
        for mnemonic_unit, right_family in zip(self.log_list, result):
            mnemonic = mnemonic_unit[0]
            mnem_res = res[mnemonic]
            self.assertIsNotNone(mnem_res)
            self.assertEqual(mnem_res.family, right_family, f'{mnemonic} failed')


class TestFamilyAssignerNode(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        test_data = os.path.join(os.path.dirname(__file__), 'data', 'sample_2.0_minimal.las')
        self.well = Well("test", new=True)
        self.dataset = WellDataset(self.well, "testds", new=True)
        import_to_db(filename=test_data, well=self.well, well_dataset=self.dataset)

    def test_family_assignment_node(self):
        log = BasicLog(self.dataset.id, log_id="SP")
        log.meta.family = None  # deleting data
        log.save()

        FamilyAssignerNode.run()
        FamilyAssignerNode.run()
        log_in_db = BasicLog(self.dataset.id, log_id='SP')
        self.assertEqual('Spontaneous Potential', log_in_db.meta.family,
                         'Log family in storage is empty')
