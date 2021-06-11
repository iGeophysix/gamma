import json

import pandas
import os

from components.database.RedisStorage import RedisStorage

SOURCE_FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'FluidMineralConstants.xlsx')
EXPORT_FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'FluidMineralConstants.json')
FLUID_MINERAL_TABLE = 'fluid_mineral_constants'

def build_fluid_mineral_constants():
    # read Excel table
    with open(SOURCE_FLUID_MINERAL_TABLE, 'rb') as f:
        df = pandas.read_excel(f, 'Constants', header=0, index_col=0)

    # export table as JSON

    s = RedisStorage()
    s.object_delete(FLUID_MINERAL_TABLE)
    s.object_set(FLUID_MINERAL_TABLE, json.dumps(df.to_dict(orient='index')))

if __name__ == '__main__':
    build_fluid_mineral_constants()
