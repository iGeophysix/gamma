import pandas
import json
import os

SOURCE_UNITS_TABLE = os.path.join(os.path.dirname(__file__), 'Units.xlsx')
EXPORT_UNITS_SYSTEM = os.path.join(os.path.dirname(__file__), 'UnitsSystem.json')


def main():
    # read Excel table
    cols = ['Dimension Name', 'BaseUnit', 'Unit', 'Scale', 'Offset', 'Drop']
    with open(SOURCE_UNITS_TABLE, 'rb') as f:
        df = pandas.read_excel(f, 'Units', header=0, usecols=cols)

    # create units database
    # unit_value = base_unit_value * k + b
    units = {}
    for r in range(len(df.index)):
        if df['Drop'][r] != 'Yes':
            base_unit = df['BaseUnit'][r].strip()
            unit = df['Unit'][r].strip()
            dimension = df['Dimension Name'][r].strip()
            unit_info = {'k': float(df['Scale'][r])}
            b = df['Offset'][r]
            if b is not None:
                b = float(b)
                if b != 0:
                    unit_info['b'] = b
            compat_units = units.setdefault(dimension, {})
            if base_unit not in compat_units:
                compat_units[base_unit] = {'k': 1.0, 'base': True}
            if unit != base_unit:
                compat_units[unit] = unit_info

    # find case-sensitive units
    delete_later = set()
    for dimension, compat_units in units.items():
        for unit in compat_units:
            lunit = unit.lower()
            for dimension2, compat_units2 in units.items():
                for unit2 in compat_units2:
                    lunit2 = unit2.lower()
                    if dimension == dimension2 and unit == unit2:   # it's me, skip
                        continue
                    assert unit != unit2, f'Units must be unique. Double entry of {dimension}."{unit}" and {dimension2}."{unit2}"'
                    if lunit == lunit2:
                        if dimension == dimension2:     # same dimension, compatible units with different case
                            unit_info = compat_units[unit]
                            unit2_info = compat_units2[unit2]
                            if unit_info['k'] == unit2_info['k'] and unit_info.get('b') == unit2_info.get('b'):  # same unit with different case
                                if (dimension, unit) not in delete_later and (dimension2, unit2) not in delete_later:
                                    if unit_info.get('base', False):    # not going to delete a base unit
                                        delete_later.add((dimension2, unit2))
                                    else:
                                        delete_later.add((dimension, unit))
                            else:   # different units with case difference
                                compat_units[unit]['case-sensitive'] = True
                        else:   # units from diferent dimensions are always different
                            compat_units[unit]['case-sensitive'] = True

    for dimension, unit in delete_later:
        del units[dimension][unit]

    # export units system JSON
    with open(EXPORT_UNITS_SYSTEM, 'w') as f:
        json.dump(units, f, sort_keys=True, indent='\t')


if __name__ == '__main__':
    main()
