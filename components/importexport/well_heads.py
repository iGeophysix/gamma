from typing import TextIO, Any

from components.domain.Well import Well


def well_heads_csv_header(raw_data: TextIO,
                          columns_names_row: int = 0,
                          units_row: int = 1,
                          wellname_column: int = 0,
                          delimiter: str = ',') -> dict:
    '''
    Function to read csv data containing markers.
    All counts are starting from 0. First row - index 0, Second row - index 1,...

    :param raw_data: File object or io.StreamIO. raw csv data as string
    :param columns_names_row: index of columns names row. Default: 0 - the first row
    :param units_row: index of row containing units. Default: 1 - the second row
    :param wellname_column: column number containing well names. Default: 0 - the first column
    :param delimiter: csv delimiter. Default: ','

    File example:
        #WellPropertyName;Elevation;X;Y;Total_depth
        #WellPropertyUnit;m;m;m;m
        51R;55.7;14205.5;33939;2046;1
        4732;62.75;14361.3;31057.09;2338;2
        4733;62.43;14354.94;31033.95;2290;3
        4734;62.48;13520.12;31803.05;2312;4
        4735;62.47;13514.02;31817.57;2295;5
        4736;59.54;12471.11;32647.65;2360;6

    '''

    _columns_row = -1 if columns_names_row is None else columns_names_row
    _units_row = -1 if units_row is None else units_row
    header = [raw_data.readline() for _ in range(max(_columns_row, _units_row) + 1)]

    # if file has no header return empty header
    if not header:
        return {}

    names = header[columns_names_row].strip().split(delimiter)
    names[wellname_column] = '__wellname__'
    names = [name if name != '' else f'Unknown{i}' for i, name in enumerate(names)]

    if units_row is not None:
        units = header[units_row].strip().split(delimiter)
    else:
        units = [""] * len(names)
    return dict(zip(names, units))


def guess_type(text: str) -> Any:
    types = [int, float, str]
    for t in types:
        try:
            return t(text)
        except ValueError as exc:
            continue


def import_well_heads_csv(raw_data: TextIO,
                          header: dict,
                          delimiter=',') -> None:
    """
    Function to read csv containing well head data.

    :param raw_data: file stream with skipped header
    :param header: file header
    :param delimiter:
    :return:

    File example:
        51R;55.7;14205.5;33939;2046;1
        4732;62.75;14361.3;31057.09;2338;2
        4733;62.43;14354.94;31033.95;2290;3
        4734;62.48;13520.12;31803.05;2312;4
        4735;62.47;13514.02;31817.57;2295;5
        4736;59.54;12471.11;32647.65;2360;6

    """

    for line in raw_data.readlines():
        data = line.strip().split(delimiter)
        data_row = {head[0]: {'value': guess_type(value), 'units': head[1]} for head, value in zip(header.items(), data)}
        well = Well(str(data_row['__wellname__']['value']), new=True)
        del data_row['__wellname__']
        well.update_meta(info=data_row)
