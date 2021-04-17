import logging
import os

import numpy as np

from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las
from utilities import my_timer

gamma_logger = logging.getLogger('gamma_logger')


# @my_timer
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

    if las_structure is None:
        raise Exception(f'Empty las structure for file "{filename}"')

    if las_structure.error_message:
        raise Exception(f'File "{las_structure.filename}" has an error "{las_structure.error_message}"')

    # We can't have dataset without a valid well
    if well is None and well_dataset is not None:
        raise Exception(f'Impossible to have a dataset without a well when importing "{filename}"')

    created_new_well = False

    if well is None:
        wellname = las_structure.required_well_entries["WELL"].value

        if not wellname:
            gamma_logger.error(f'File "{las.filename}" has no valid WELL field.')
            return

        well = Well(wellname, new=True)
        created_new_well = True

    if well_dataset is None:
        datasetname = os.path.basename(las_structure.filename)
        well_dataset = WellDataset(well, datasetname, new=True)

    raw_curves = las_structure.data
    md_key = list(raw_curves.keys())[0]  # TODO: Is it always #0?
    md_values = raw_curves[md_key]

    for log, values in raw_curves.items():
        if log == md_key:
            continue
        this_log = BasicLog(dataset_id=well_dataset.id, id=log)
        this_log.values = np.array(tuple(zip(md_values, values)))
        this_log.meta = this_log.meta | las_structure.logs_info()[log]
        this_log.save()
        this_log.history = f"Loaded from {las_structure.filename}"
        this_log.save()

    # write meta-information about this well
    well_info = las_structure.well_info()
    if created_new_well:
        well.info = well_info

    return well_info
