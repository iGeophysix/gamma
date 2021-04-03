import unittest

from components.importexport.FamilyAssigner import FamilyAssigner


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
