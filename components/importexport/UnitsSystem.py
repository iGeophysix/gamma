import json
import numpy as np
from numbers import Number

UNIT_SYSTEM_PATH = 'components/importexport/rules/UnitsSystem.json'


class UnitsSystem:
    '''
    Functionality for data units conversion.
    '''
    def __init__(self):
        with open(UNIT_SYSTEM_PATH, 'r') as f:
            self._db = json.load(f)
        self._unit_dim = {}         # unit -> dimention
        self._dim_base_unit = {}    # dimention -> base unit
        self._ci_unit_unit = {}     # lower_case_unit -> unit. Search for case-insensitive units
        for dimention, units in self._db.items():
            for unit, unit_info in units.items():
                self._unit_dim[unit] = dimention
                if not unit_info.get('case-sensitive', False):
                    self._ci_unit_unit[unit.lower()] = unit
                if unit_info.get('base', False):
                    self._dim_base_unit[dimention] = unit

    def unit_dimention(self, unit: str):
        '''
        Get dimention name of the unit.
        Return None if unit is unknown.
        '''
        unit = self._ci_unit_unit.get(unit.lower(), unit)    # try case-insensitive search
        return self._unit_dim.get(unit)

    def dimention_base_unit(self, dimention: str):
        '''
        Get base unit for the dimention.
        Return None if dimention is unknown.
        '''
        return self._dim_base_unit.get(dimention)

    def unit_base_unit(self, unit: str):
        '''
        Get base unit for the unit.
        Return None if unit is unknown.
        '''
        dim = self.unit_dimention(unit)
        return self.dimention_base_unit(dim)

    def known_unit(self, unit: str) -> bool:
        '''
        Check that unit is known.
        '''
        return self.unit_dimention(unit) is not None

    def convertable_units(self, unit1: str, unit2: str) -> bool:
        '''
        Check unit converting capability.
        '''
        dim1 = self.unit_dimention(unit1)
        dim2 = self.unit_dimention(unit2)
        return dim1 == dim2 and dim1 is not None

    def identical_units(self, unit1: str, unit2: str) -> bool:
        '''
        Check for units equality.
        '''
        return self.convertable_units(unit1, unit2) and self._unit_kb(unit1) == self._unit_kb(unit2)    # same base unit, equal k and b

    def _unit_kb(self, unit: str) -> (float, float):
        '''
        Get scale coefficient k and additive constant b for the unit.
        '''
        unit = self._ci_unit_unit.get(unit.lower(), unit)   # try case-insensitive search
        unit_info = self._db[self.unit_dimention(unit)][unit]
        return unit_info['k'], unit_info.get('b', 0)

    def convert(self, data, unit_from: str, unit_to: str):
        '''
        Perform unit conversion of the data array or single value.
        Array mode is preferable for batch conversion.
        '''
        array_mode = not isinstance(data, Number)
        if array_mode:
            data = np.array(data)
        if not self.convertable_units(unit_from, unit_to):
            if array_mode:
                data.fill(np.nan)
            else:
                data = np.nan
            return data
        else:
            k1, b1 = self._unit_kb(unit_from)
            k2, b2 = self._unit_kb(unit_to)
            return ((data - b1) / k1) * k2 + b2
