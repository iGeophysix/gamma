import pandas
import os

SOURCE_FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'FluidMineralConstants.xlsx')
EXPORT_FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'FluidMineralConstants.json')


def main():
    # read Excel table
    with open(SOURCE_FLUID_MINERAL_TABLE, 'rb') as f:
        df = pandas.read_excel(f, 'Constants', header=0, index_col=0)

    # export table as JSON
    df.to_json(EXPORT_FLUID_MINERAL_TABLE, orient='index', indent=4)


if __name__ == '__main__':
    main()
