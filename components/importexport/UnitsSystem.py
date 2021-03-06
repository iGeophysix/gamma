import pickle
from typing import Tuple, Union, Iterable

import numpy as np

from components.database.RedisStorage import RedisStorage


class UnitConversionError(Exception):
    pass


class UnitsSystem:
    '''
    Functionality for data units conversion.
    '''

    def __init__(self):
        s = RedisStorage()
        unit_system = pickle.loads(s.object_get('UnitSystem'))
        self._db = unit_system['_db']
        self._unit_dim = unit_system['_unit_dim']  # unit -> dimension
        self._dim_base_unit = unit_system['_dim_base_unit']  # dimension -> base unit
        self._ci_unit_unit = unit_system['_ci_unit_unit']  # lower_case_unit -> unit. Search for case-insensitive units
        self._unit_renaming = unit_system['_unit_renaming']  # list of (input_unit: re.Pattern, rename_to_unit: str)

    def unit_dimension(self, unit: str):
        '''
        Get dimension name of the unit.
        Return None if unit is unknown.
        '''
        if not unit:
            return None
        unit = self._ci_unit_unit.get(unit.lower(), unit)  # try case-insensitive search
        return self._unit_dim.get(unit)

    def dimension_base_unit(self, dimension: str):
        '''
        Get base unit for the dimension.
        Return None if dimension is unknown.
        '''
        return self._dim_base_unit.get(dimension)

    def unit_base_unit(self, unit: str):
        '''
        Get base unit for the unit.
        Return None if unit is unknown.
        '''
        dim = self.unit_dimension(unit)
        return self.dimension_base_unit(dim)

    def known_unit(self, unit: str) -> bool:
        '''
        Check that unit is known.
        '''
        return self.unit_dimension(unit) is not None

    def convertable_units(self, unit1: str, unit2: str) -> bool:
        '''
        Check unit converting capability.
        '''
        dim1 = self.unit_dimension(unit1)
        dim2 = self.unit_dimension(unit2)
        return dim1 == dim2 and dim1 is not None

    def identical_units(self, unit1: str, unit2: str) -> bool:
        '''
        Check for units equality.
        '''
        return self.convertable_units(unit1, unit2) and self._unit_kb(unit1) == self._unit_kb(unit2)  # same base unit, equal k and b

    def _unit_kb(self, unit: str) -> Tuple[float, float]:
        '''
        Get scale coefficient k and additive constant b for the unit.
        '''
        unit = self._ci_unit_unit.get(unit.lower(), unit)  # try case-insensitive search
        unit_info = self._db[self.unit_dimension(unit)][unit]
        return unit_info['k'], unit_info.get('b', 0)

    def convert(self, data: Union[np.number, np.ndarray], unit_from: str, unit_to: str) -> Union[np.number, np.ndarray]:
        '''
        Perform unit conversion of the data array or single value.
        Array mode is preferable for batch conversion.
        '''
        if isinstance(data, (list, tuple)):
            data = np.array(data)
        if not self.convertable_units(unit_from, unit_to):
            raise UnitConversionError(f'Impossible to convert {unit_from} to {unit_to}')
        else:
            k1, b1 = self._unit_kb(unit_from)
            k2, b2 = self._unit_kb(unit_to)
            return ((data - b1) / k1) * k2 + b2

    def fix_naming(self, unit: str) -> str:
        '''
        Make unit naming standard.
        '''
        for re_unit, rename_to in self._unit_renaming:
            if re_unit.fullmatch(unit):
                return rename_to
        return unit
