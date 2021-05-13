from datetime import datetime
from typing import Iterable

import lasio
import numpy as np

from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset


def create_las_file(well_name: str, paths_to_logs: Iterable[tuple[str, str]]) -> lasio.LASFile:
    well = Well(well_name)
    logs = {}
    for path_to_log in paths_to_logs:
        ds = WellDataset(well, path_to_log[0])
        logs[f"{ds.name}__{path_to_log[1]}"] = BasicLog(ds.id, path_to_log[1])

    # define depth reference
    step = np.min([log.meta['basic_statistics']['avg_step'] for log in logs.values()])
    min_depth = np.min([log.meta['basic_statistics']['min_depth'] for log in logs.values()])
    max_depth = np.max([log.meta['basic_statistics']['max_depth'] for log in logs.values()])
    new_reference = np.arange(min_depth, max_depth, step)

    logs_interpolated = {log_path: log.interpolate(new_reference) for log_path, log in logs.items()}

    # create las
    las = lasio.LASFile()
    las.well.WELL = well_name
    las.well.STRT = min_depth
    las.well.STOP = max_depth
    las.well.STEP = step
    las.well.DATE = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    las.add_curve('DEPT', logs_interpolated[list(logs_interpolated.keys())[0]][:, 0])
    for log_path, log_values in logs_interpolated.items():
        log_meta = logs[log_path].meta.asdict()
        las.add_curve(logs[log_path].name, log_values[:, 1], unit=log_meta.get('units', ''), descr=log_meta.get('family', ''))

    return las
