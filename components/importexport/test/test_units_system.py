import unittest

import numpy as np

from components.importexport.UnitsSystem import UnitsSystem, UnitConversionError


class TestUnitSystem(unittest.TestCase):

    def setUp(self):
        self.units = UnitsSystem()

    def test_known_units(self):
        self.assertTrue(self.units.known_unit('km'))
        self.assertTrue(self.units.known_unit('KM'))
        self.assertFalse(self.units.known_unit('KKM'))

    def test_identical_units(self):
        self.assertTrue(self.units.identical_units('Ohm.m', 'ohmm'))
        self.assertTrue(self.units.identical_units('cc', 'cm3'))
        self.assertFalse(self.units.identical_units('mV', 'MV'))

    def test_unit_dimension(self):
        self.assertEqual(self.units.unit_dimension('c/m3'), 'AnglePerVolume')
        self.assertEqual(self.units.unit_dimension('C/m3'), 'ElectricChargePerVolume')

    def test_unit_base_unit(self):
        self.assertEqual(self.units.unit_base_unit('um'), 'm')
        self.assertEqual(self.units.unit_base_unit('mV'), 'V')
        self.assertIsNone(self.units.unit_base_unit('UNK'))

    def test_convertable_units(self):
        self.assertTrue(self.units.convertable_units('m', 'in'))
        self.assertFalse(self.units.convertable_units('g/cm3', 'Pa'))

    def test_impossible_conversion(self):
        with self.assertRaises(UnitConversionError):
            self.units.convert(1, 'kg', 'cm')

    def test_conversion(self):
        temp_degC_list = [-5, np.nan, 34.67, 58]
        temp_degC_np = np.array(temp_degC_list)
        temp_degF_np = np.array([23.0, np.nan, 94.406, 136.4])
        # array
        answ = self.units.convert(temp_degC_list, 'degC', 'degF')
        self.assertTrue(np.allclose(answ, temp_degF_np, equal_nan=True))
        answ = self.units.convert(temp_degC_np, 'degC', 'degF')
        self.assertTrue(np.allclose(answ, temp_degF_np, equal_nan=True))
        # single value
        self.assertAlmostEqual(self.units.convert(float(temp_degC_np[0]), 'degC', 'degF'), temp_degF_np[0])
        self.assertAlmostEqual(self.units.convert(int(temp_degC_np[0]), 'degC', 'degF'), temp_degF_np[0])
        self.assertTrue(np.isnan(self.units.convert(np.nan, 'm', 'mm')))

    def test_fix_units_naming(self):
        self.assertEqual(self.units.fix_naming('MV'), 'mV')
        self.assertEqual(self.units.fix_naming('F'), 'ft')
