import unittest

from components.importexport.FamilyAssigner import FamilyAssigner

class TestFamilyAssignment(unittest.TestCase):
    def setUp(self):
        self.fa = FamilyAssigner()

    def test_families_assigned(self):
        mnem_list = ['GR.NORM', 'PS', 'RB', 'BS', 'RAW-TNPH', 'FCAZ', 'HAZI', 'CALI 2']

        result1 = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing',
                    'Outside Diameter', 'Thermal Neutron Porosity', 'Z Acceleration',
                    'Hole Azimuth', 'Borehole Diameter']

        result2 = ['Gamma Ray', 'Spontaneous Potential', 'Relative Bearing',
                    'Nom Borehole Diameter', 'Thermal Neutron Porosity',
                    'Z Acceleration', 'Hole Azimuth', 'Borehole Diameter']

        for n in range(len(mnem_list)):
            self.assertEqual(self.fa.assign_family(mnem_list[n], one_best=True)[0], result1[n])

        self.assertEqual([r[0] for r in self.fa.assign_families(mnem_list).values()], result2)


if __name__ == '__main__':
    unittest.main()
