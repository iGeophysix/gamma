import time
from typing import Iterable

from celery_conf import app
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine import Engine
from components.importexport import las
from components.importexport.FamilyAssigner import FamilyAssigner
from components.importexport.las import import_to_db
from components.importexport.las_importexport import LasExportNode
from components.petrophysics.best_log_detection import rank_logs
from components.petrophysics.curve_operations import get_basic_curve_statistics, rescale_curve, LogResolutionNode
from components.petrophysics.log_reconstruction import LogReconstructionNode
from components.petrophysics.log_splicing import SpliceLogsNode
from components.petrophysics.porosity import PorosityFromDensityNode
from components.petrophysics.run_detection import detect_runs_in_well
from components.petrophysics.saturation import SaturationArchieNode
from components.petrophysics.shale_volume import ShaleVolumeLarionovOlderRockNode, ShaleVolumeLarionovTertiaryRockNode, ShaleVolumeLinearMethodNode
from components.petrophysics.volumetric_model import VolumetricModelSolverNode


@app.task
def async_run_workflow(workflow_id: str = None):
    workflow = Engine()
    workflow.start()


@app.task
def async_read_las(wellname: str = None, datasetname: str = None, filename: str = None, las_data: str = None):
    start = time.time()
    if filename is not None and las_data is None:
        well = Well(wellname)
        dataset = WellDataset(well, datasetname)
        import_to_db(filename=filename, well=well, well_dataset=dataset)
    elif las_data is not None:
        las_structure = las.parse(filename=filename, data=las_data)
        well = Well(wellname) if wellname is not None else None
        dataset = WellDataset(well, datasetname) if datasetname is not None else None
        import_to_db(las_structure=las_structure, well=well, well_dataset=dataset)
    else:
        raise Exception('Incorrect function call')
    end = time.time()
    return {'filename': filename, 'task_time': end - start}


@app.task
def async_rescale_log(wellname: str, datasetname: str, logs: dict) -> None:
    '''
    Apply asynchronous normalization of curves in a dataset
    :param wellname: well name as string
    :param datasetname: list of dataset names to process. If None then use all datasets for the well
    :param logs: list of logs names to process. If None then use all logs for the dataset
        {"GR": {"min_value":0,"max_value":150, "output":"GR_norm"}, "RHOB": {"min_value":1.5,"max_value":2.5, "output":"RHOB_norm"},}
    :return:
    '''

    well = Well(wellname)
    dataset = WellDataset(well, datasetname)
    well_logs = {log: BasicLog(dataset.id, log) for log in logs.keys()}

    for log in well_logs:
        params = logs[log]
        new_log = BasicLog(dataset.id, params['output'])
        new_log.values = rescale_curve(well_logs[log].values, params["min_value"], params["max_value"])
        new_log.meta = log.meta
        new_log.meta.append_history(f"Normalized curve derived from {wellname}->{datasetname}->{log}")
        new_log.save()


@app.task
def async_get_basic_log_stats(wellname: str, datasetnames: list[str] = None, logs: list[str] = None) -> None:
    """
    This procedure calculates basic statistics (e.g. mean, gmean, stdev, etc).
    Returns nothing. All results are stored in each log meta info.
    :param wellname: well name as string
    :param datasetnames: list of dataset names to process. If None then use all datasets for the well
    :param logs: list of logs names to process. If None then use all logs for the dataset
    """
    w = Well(wellname)
    if datasetnames is None:
        datasetnames = w.datasets

    # get all data from specified well and datasets
    for datasetname in datasetnames:
        d = WellDataset(w, datasetname)

        log_names = d.log_list if logs is None else logs

        for log_name in log_names:
            log = BasicLog(d.id, log_name)
            log.meta.update({'basic_statistics': get_basic_curve_statistics(log.values)})
            log.save()


@app.task
def async_log_resolution(wellname: str, datasetnames: list[str] = None, logs: list[str] = None) -> None:
    """
    This procedure calculates log resolution.
    Algorithm: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/a/Log-Resolution-Evaluation-ZYfMr18R4U2
    Returns nothing. All results are stored in each log meta info.
    :param wellname: well name as string
    :param datasetnames: list of dataset names to process. If None then use all datasets for the well
    :param logs: list of logs names to process. If None then use all logs for the dataset
    """
    node = LogResolutionNode()
    node.calculate_for_log(wellname, datasetnames, logs)


@app.task
def async_split_by_runs(wellname: str, depth_tolerance: float = 50) -> None:
    """
    Assign RUN id_s to all logs in the well.
    Returns nothing. All results (RUN ids and basic_stats) are stored in each log meta info.
    :param wellname: well name as string
    :param depth_tolerance: distance to consider as acceptable difference in depth in one run
    """
    w = Well(wellname)
    detect_runs_in_well(w, depth_tolerance)


@app.task
def async_recognize_family(wellname: str, datasetnames: list[str] = None, lognames: list[str] = None) -> None:
    """
    Recognize log family in well datasets
    :param wellname:
    :param datasetnames:
    :return:
    """
    fa = FamilyAssigner()
    w = Well(wellname)
    if datasetnames is None:
        datasetnames = [ds for ds in w.datasets if ds != 'LQC']

    for datasetname in datasetnames:
        wd = WellDataset(w, datasetname)

        log_list = wd.log_list if lognames is None else lognames

        for log in log_list:
            l = BasicLog(wd.id, log)
            result = fa.assign_family(l.name, l.meta.units)
            if result is not None:
                l.meta.family = result.family
                l.meta.family_assigner = {'reliability': result.reliability, 'unit_class': result.dimension, 'logging_company': result.company, 'logging_tool': result.tool}
            else:
                l.meta.family = l.meta.family_assigner = None
            l.save()


@app.task
def async_splice_logs(wellname: str,
                      datasetnames: list[str] = None,
                      logs: list[str] = None,
                      tags: list[str] = None,
                      output_dataset_name: str = 'Spliced') -> None:
    """
    Async method to splice logs. Takes  logs in datasets and outputs it into a
    specified output dataset
    :param wellname: Well name as string
    :param datasetnames: Datasets' name as list of strings. If None then uses all datasets
    :param logs: Logs' names as list of string. If None then uses all logs available in datasets
    :param output_dataset_name: Name of output dataset
    """
    SpliceLogsNode.calculate_for_well(wellname,
                                      datasetnames,
                                      logs,
                                      tags,
                                      output_dataset_name)


@app.task
def async_detect_best_log(log_paths: tuple[tuple[str, str]]) -> None:
    '''
    Celery task to run best log detection from BestLogDetectionNode
    :param log_paths:
    :return:
    '''
    logs = {log: BasicLog(log[0], log[1]) for log in log_paths}
    logs_meta = {log: log.meta for log in logs.values()}
    best_log, new_meta = rank_logs(logs_meta)
    for log, values in new_meta.items():
        log.meta.update(values)
        log.meta.add_tags('processing')
        log.save()


@app.task
def async_calculate_volumetric_model(well_name, model_components):
    vm = VolumetricModelSolverNode()
    vm.calculate_for_well(well_name, model_components)


@app.task
def async_calculate_porosity_from_density(well_name, rhob_matrix: float = None, rhob_fluid: float = None, output_log_name: str = 'VSH_GR'):
    vm = PorosityFromDensityNode()
    vm.calculate_for_well(well_name, rhob_matrix, rhob_fluid, output_log_name)


@app.task
def async_calculate_shale_volume(well_name: str, algorithm: str = 'Linear', gr_matrix: float = None, gr_shale: float = None, output_log_name: str = 'VSH_GR'):
    if algorithm == 'ShaleVolumeLarionovOlderRockNode':
        node = ShaleVolumeLarionovOlderRockNode()
    elif algorithm == 'ShaleVolumeLarionovTertiaryRockNode':
        node = ShaleVolumeLarionovTertiaryRockNode()
    elif algorithm == 'ShaleVolumeLinearMethodNode':
        node = ShaleVolumeLinearMethodNode()
    else:
        raise ValueError(f"Unknown kind of algorithm: {algorithm}."
                         f"Acceptable values: 'ShaleVolumeLinearMethodNode', 'ShaleVolumeLarionovTertiaryRockNode', 'ShaleVolumeLarionovOlderRockNode'.")
    node.calculate_for_well(well_name, gr_matrix, gr_shale, output_log_name)


@app.task
def async_export_well_to_s3(destination: str, wellname, datasetname: str = 'LQC', logs: Iterable[str] = None):
    '''
    Celery task to export data to LAS file and put it into a folder on S3
    :param destination: name of folder in public bucket
    :param wellname: well name to export
    :param datasetname: dataset name to export. Default: 'LQC'
    :param logs: log names (Iterable) to export. Default: None - export all logs
    '''
    node = LasExportNode()
    node.export_well_dataset(destination, wellname, datasetname, logs)


@app.task
def async_log_reconstruction(model, well_name, log_families_to_train, log_family_to_predict, log_to_predict_units):
    node = LogReconstructionNode()
    node.calculate_for_well(model, well_name, log_families_to_train, log_family_to_predict, log_to_predict_units)


@app.task
def async_saturation_archie(well_name, a, m, n, rw, output_log_name):
    node = SaturationArchieNode()
    node.calculate_for_item(well_name, a, m, n, rw, output_log_name)
