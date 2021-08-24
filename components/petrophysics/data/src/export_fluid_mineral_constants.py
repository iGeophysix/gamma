import json
import pandas
import os

from components.database.RedisStorage import RedisStorage, FLUID_MINERAL_TABLE_NAME

SOURCE_FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'FluidMineralConstants.xlsx')


def build_fluid_mineral_constants():
    # read Excel table
    with open(SOURCE_FLUID_MINERAL_TABLE, 'rb') as f:
        df = pandas.read_excel(f, 'Constants', header=0, index_col=0)
    df.fillna(0, inplace=True)

    # export table as JSON
    s = RedisStorage()
    s.object_set(FLUID_MINERAL_TABLE_NAME, json.dumps(df.to_dict(orient='index')))


if __name__ == '__main__':
    build_fluid_mineral_constants()
