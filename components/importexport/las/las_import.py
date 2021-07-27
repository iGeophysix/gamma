import logging
import os

import numpy as np

from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las
from components.importexport.UnitsSystem import UnitsSystem
from settings import DEFAULT_DEPTH_UNITS

gamma_logger = logging.getLogger('gamma_logger')


class LoadingException(Exception):
    pass


def import_to_db(filename: str = None,
                 las_structure=None,
                 well: Well = None,
                 well_dataset: WellDataset = None) -> dict:
    """
    Import LAS file and assign its values to the given well and dataset.
    :param las_structure: optional already parsed LasStructure.
    :param well: optional existing well for importing the las data.
    :param well_dataset: optional existing dataset where data is overwritten
    :return: well info from las file header
    """

    if filename:
        las_structure = las.parse(filename)

    if las_structure is None or not las_structure.valid():
        raise LoadingException(f'Empty las structure for file "{filename}"')

    if las_structure.error_message:
        raise LoadingException(f'File "{las_structure.filename}" has an error "{las_structure.error_message}"')

    # We can't have dataset without a valid well
    if well is None and well_dataset is not None:
        raise LoadingException(f'Impossible to have a dataset without a well when importing "{las_structure.filename}"')

    created_new_well = False

    if well is None:
        wellname = las_structure.required_well_entries["WELL"].value

        if not wellname:
            gamma_logger.error(f'File "{las_structure.filename}" has no valid WELL field.')
            raise LoadingException(f'File "{las_structure.filename}" has no valid WELL field.')

        well = Well(wellname)
        created_new_well = False
        if not well.exists():
            well = Well(wellname, new=True)
            created_new_well = True

    if well_dataset is None:
        datasetname = os.path.basename(las_structure.filename)
        well_dataset = WellDataset(well, datasetname, new=True)

    well_dataset_info = well_dataset.meta
    well_dataset_info['source'] = filename
    well_dataset.meta = well_dataset_info

    raw_curves = las_structure.data
    md_key = list(raw_curves.keys())[0]  # TODO: Is it always #0?
    md_values = raw_curves[md_key]

    unit_converter = UnitsSystem()

    def fix_unit_naming(las_section_meta):
        'Fix non-standard units naming'
        for meta in las_section_meta.values():
            units = meta.get('units')
            if units is not None:
                meta['units'] = unit_converter.fix_naming(units)

    logs_info = las_structure.logs_info()
    fix_unit_naming(logs_info)

    # convert depth reference to default depth units
    md_values = unit_converter.convert(md_values, logs_info[md_key]['units'], DEFAULT_DEPTH_UNITS)

    for log, values in raw_curves.items():
        this_log = BasicLog(dataset_id=well_dataset.id, log_id=log)
        this_log.values = np.array(tuple(zip(md_values, values)))
        this_log.meta = logs_info[log]
        this_log.meta.depth_reference = md_key
        this_log.meta.source = las_structure.filename
        this_log.meta.add_tags('raw')
        if log == md_key:
            this_log.meta.add_tags('main_depth')
            this_log.meta.units = DEFAULT_DEPTH_UNITS
        this_log.save()

    # write meta-information about this well
    well_info = las_structure.well_info()
    fix_unit_naming(well_info)
    well_dataset.meta = well_info
    if created_new_well:
        well.meta = well_info

    return well_info
