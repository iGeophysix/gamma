import os.path
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.FamilyAssigner import FamilyAssigner, FamilyAssignerNode
from components.importexport.las import import_to_db


class TestFamilyAssignment(unittest.TestCase):
    def setUp(self):
        self.fa = FamilyAssigner()
        self.mnem_list = ['GR.NORM', 'PS', 'RB', 'BS', 'RAW-TNPH', 'FCAZ', 'HAZI', 'CALI 2']

    def test_one_by_one(self):
        result = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing',
                  'Outside Diameter', 'Thermal Neutron Porosity', 'Z Acceleration',
                  'Hole Azimuth', 'Borehole Diameter']
        for n, mnemonic in enumerate(self.mnem_list):
            res = self.fa.assign_family(mnemonic, one_best=True)
            self.assertIsNotNone(res)
            family, dimension, rank = res
            self.assertEqual(family, result[n])

    def test_batch(self):
        result = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing',
                  'Nom Borehole Diameter', 'Thermal Neutron Porosity', 'Z Acceleration',
                  'Hole Azimuth', 'Borehole Diameter']
        res = self.fa.assign_families(self.mnem_list)
        for mnemonic, right_family in zip(self.mnem_list, result):
            mnem_res = res[mnemonic]
            self.assertIsNotNone(mnem_res)
            family, dimension, rank = mnem_res
            self.assertEqual(family, right_family)


class TestFamilyAssignerNode(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        test_data = os.path.join(os.path.dirname(__file__), 'data', 'sample_2.0_minimal.las')
        self.well = Well("test", new=True)
        self.dataset = WellDataset(self.well, "testds", new=True)
        import_to_db(filename=test_data, well=self.well, well_dataset=self.dataset)

    def test_family_assignment_node(self):
        l = BasicLog(self.dataset.id, log_id="SP")
        l.meta.family = None  # deleting data
        l.save()

        fa = FamilyAssignerNode()
        fa.run()
        log_in_db = BasicLog(self.dataset.id, log_id='SP')
        self.assertEqual('Spontaneous Potential', log_in_db.meta.family, 'Log family in storage is empty')
