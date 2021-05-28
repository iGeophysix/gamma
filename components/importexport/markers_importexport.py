from collections import defaultdict
from typing import TextIO

import numpy as np

from components.domain.Log import MarkersLog
from components.domain.MarkersSet import MarkersSet
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from settings import DEFAULT_MISSING_VALUE, DEFAULT_MARKERS_NAME


def import_markers_csv(raw_data: TextIO,
                       headers_rows: int = 2,
                       units_row: int = 1,
                       well_column=0,
                       markers_set_columns=1,
                       depth_column=2,
                       zone_column=3,
                       missing_value=DEFAULT_MISSING_VALUE,
                       delimiter=',') -> None:
    '''
    Function to read csv data containing markers.
    All counts are starting from 0. First row - index 0, Second row - index 1,...

    :param raw_data: File object or io.StreamIO. raw csv data as string
    :param headers_rows: number of header rows to skip, Default: 2
    :param units_row: number of row containing units (must be within the header)
    :param well_column: number of column containing well names. Default: 0
    :param markers_set_columns: number of column containing markers set name. Default: 1
    :param depth_column: number of column containing depth reference. Default: 2
    :param zone_column: number of column containing zone name. Default: 3
    :param missing_value: value to assume as empty or missing. Default: settings.DEFAULT_MISSING_VALUE
    :param delimiter: csv file delimiter. Default: ','
    :return: Nothing

    Input file example:
            wellName,datasetName,MD,ZoneName
            ,,m,
            100,Stratigraphy,3083.6,ZoneA
            100,Stratigraphy,3131.056,-9999
            100,Stratigraphy,3143.901,ZoneB
            100,Stratigraphy,3184.4,-9999
            100,Stratigraphy,3208.8,ZoneC
            100,Stratigraphy,3231.682,ZoneD
            100,Stratigraphy,3246.2,-9999
            100,Stratigraphy,3248.465,ZoneE
            100,Stratigraphy,3279.401,-9999
            101,Stratigraphy,2490.255,ZoneA
            101,Stratigraphy,2539.808,-9999
            101,Stratigraphy,2557.229,ZoneB
            101,Stratigraphy,2594.811,-9999
            101,Stratigraphy,2618.304,ZoneC
            101,Stratigraphy,2639.62,ZoneD
            101,Stratigraphy,2656.258,-9999
            101,Stratigraphy,2659.665,ZoneE
            101,Stratigraphy,2678.038,-9999
    '''
    if units_row > headers_rows:
        raise ValueError(f"Units row number must be lower or equal")
    parsed_data = {
        'depth_units': None,
        'wells': defaultdict(lambda: defaultdict(list)),
        'marker_sets': defaultdict(set),
    }
    if headers_rows > 0:
        header = [raw_data.readline() for _ in range(headers_rows)]
        if units_row >= 0:
            parsed_data['depth_units'] = header[units_row].split(delimiter)[depth_column]

    for line in raw_data.readlines():
        data = line.strip().split(delimiter)
        well_name = data[well_column]
        marker_set_name = data[markers_set_columns]

        depth = data[depth_column]
        zone_name = data[zone_column] if data[zone_column] != missing_value else None

        # if not zone_name in parsed_data['marker_sets'][marker_set_name]:
        parsed_data['marker_sets'][marker_set_name].update((zone_name,))

        parsed_data['wells'][well_name][marker_set_name].append((depth, zone_name))

    # check if MarkerSets exists and complete
    marker_sets = {}
    for marker_set_name, zones in parsed_data['marker_sets'].items():
        ms = MarkersSet(marker_set_name)
        for zone in zones:
            ms.append(zone)
        marker_sets[marker_set_name] = ms

    # prepare marker logs
    marker_logs = []
    for well_name, zone_data in parsed_data['wells'].items():
        well = Well(well_name, new=True)
        ds = WellDataset(well, DEFAULT_MARKERS_NAME, new=True)
        for ms_name, markers in zone_data.items():
            ms = marker_sets[ms_name]
            ms.add_well(well.id)

            marker_log = MarkersLog(ds.id, ms_name)
            marker_log.values = np.asarray([(float(depth), ms[zone_name]) for depth, zone_name in markers], dtype=np.dtype(float, int))
            marker_log.meta.units = parsed_data['depth_units']
            marker_logs.append(marker_log)

    # save all
    for marker_set in marker_sets.values():
        marker_set.save()
    for marker_log in marker_logs:
        marker_log.save()
