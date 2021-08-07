import datetime
import logging
import time
from typing import Iterable, Optional

from billiard.exceptions import SoftTimeLimitExceeded
from celery.exceptions import TaskRevokedError

from components.database.RedisStorage import RedisStorage
from components.database.tasks import *
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine import Engine
from components.engine.workflow import Workflow
from components.importexport import las
from components.importexport.FamilyAssigner import FamilyAssignerNode
from components.importexport.las import import_to_db
from components.importexport.las_importexport import LasExportNode
from components.petrophysics.best_log_detection import BestLogDetectionNode
from components.petrophysics.curve_operations import (BasicStatisticsNode,
                                                      rescale_curve,
                                                      LogResolutionNode)
from components.petrophysics.log_reconstruction import LogReconstructionNode
from components.petrophysics.log_splicing import SpliceLogsNode
from components.petrophysics.porosity import PorosityFromDensityNode
from components.petrophysics.run_detection import RunDetectionNode
from components.petrophysics.saturation import SaturationArchieNode
from components.petrophysics.shale_volume import (ShaleVolumeLarionovOlderRockNode,
                                                  ShaleVolumeLarionovTertiaryRockNode,
                                                  ShaleVolumeLinearMethodNode)
from components.petrophysics.volumetric_model import VolumetricModelSolverNode

logger = logging.getLogger('CELERY_MASTER')


@app.task
def async_run_workflow(workflow_id: str = None):
    s = RedisStorage()
    task_id = async_run_workflow.request.id
    start_time = datetime.datetime.now()
    result = {
        "finished": False,
        "status_text": '',
        "nodes": [],
        "steps": {
            'completed': 0,
            'total': 0
        }
    }

    try:
        while True:
            lock = s.redlock.lock("engine_lock", 1000 * 60 * 5)
            if lock:
                s.table_key_set('engine', task_id, start_time.isoformat())
                break
            else:
                for engine_task_id in s.table_keys('engine'):
                    logger.info(f'Current Task_id: {task_id} terminating Engine calculation of task_id: {engine_task_id}')
                    app.control.revoke(engine_task_id, terminate=True, signal='SIGUSR1')
                time.sleep(0.1)

        workflow = Workflow(workflow_id if workflow_id is not None else 'default')
        engine = Engine()
        result.update(engine.start(task_id, workflow))

        app.send_task('components.database.RedisStorage.build_log_meta_fields_index', ())
        app.send_task('components.database.RedisStorage.build_dataset_meta_fields_index', ())
        app.send_task('components.database.RedisStorage.build_well_meta_fields_index', ())

    except SoftTimeLimitExceeded:
        result['status_text'] = f"The task {task_id} was revoked or breached soft time limit"
        logger.info(result['status_text'])
    except TaskRevokedError:
        result['status_text'] = f"The task {task_id} was revoked"
        logger.warning(result['status_text'])
    except Exception as exc:
        result['status_text'] = repr(exc)
        logger.error(result['status_text'])
    finally:
        if s.table_key_exists('engine', task_id):
            s.table_key_delete('engine', task_id)
        s.redlock.unlock(lock)
    return result


@app.task
def async_read_las(wellname: str = None, datasetname: str = None, filename: str = None, las_data: str = None):
    start = time.time()
    if filename is not None and las_data is None:
        well = Well(wellname)
        dataset = WellDataset(well, datasetname)
        import_to_db(filename=filename, well=well, well_dataset=dataset)
    elif las_data is not None:
        las_structure = las.parse(filename=filename.replace('\\', '/'), data=las_data)  # replace makes path cross-platform
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
def async_get_basic_log_stats(wellname: str,
                              datasetnames: Optional[list[str]] = None,
                              lognames: Optional[list[str]] = None) -> None:
    """
    This procedure calculates basic statistics (e.g. mean, gmean, stdev, etc).
    Returns nothing. All results are stored in each log meta info.
    :param wellname: well name as string
    :param datasetnames: list of dataset names to process. If None then use all datasets for the well
    :param logs: list of logs names to process. If None then use all logs for the dataset
    """

    BasicStatisticsNode.run_for_item(wellname=wellname,
                                     datasetnames=datasetnames,
                                     lognames=lognames)


@app.task
def async_log_resolution(dataset_id: str,
                         log_id: str) -> None:
    """
    This procedure calculates log resolution.
    Algorithm: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/a/Log-Resolution-Evaluation-ZYfMr18R4U2
    Returns nothing. All results are stored in each log meta info.
    :param dataset_id: Dataset id to process.
    :param log_id: Log id  to process.
    """
    LogResolutionNode.run_for_item(dataset_id=dataset_id, log_id=log_id)


@app.task
def async_split_by_runs(wellname: str, depth_tolerance: float = 50) -> None:
    """
    Assign RUN id_s to all logs in the well.
    Returns nothing. All results (RUN ids and basic_stats) are stored in each log meta info.
    :param wellname: well name as string
    :param depth_tolerance: distance to consider as acceptable difference in depth in one run
    """
    RunDetectionNode.run_for_item(wellname=wellname,
                                  depth_tolerance=depth_tolerance)


@app.task
def async_recognize_family(wellname: str) -> None:
    """
    Recognize log family in well datasets
    :param wellname:
    :return:
    """
    FamilyAssignerNode.run_for_item(wellname=wellname)


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
    SpliceLogsNode.run_for_item(wellname=wellname,
                                datasetnames=datasetnames,
                                logs=logs,
                                tags=tags,
                                output_dataset_name=output_dataset_name)


@app.task
def async_detect_best_log(log_type: str,
                          log_paths: tuple[tuple[str, str]],
                          additional_logs_paths: Optional[tuple[tuple[str, str]]]) -> None:
    '''
    Celery task to run best log detection from BestLogDetectionNode
    :param log_type: 'general' for all logs and 'resistivity' for resistivity logs
    :param log_paths: list of (dataset_id, log_id) - best log candidates
    :param additional_logs_paths: list of (dataset_id, log_id) - additional logs to make statistics represenatative
    '''
    BestLogDetectionNode.run_for_item(log_type=log_type,
                                      log_paths=log_paths,
                                      additional_logs_paths=additional_logs_paths)


@app.task
def async_calculate_volumetric_model(well_name, model_components):
    VolumetricModelSolverNode.run_for_item(well_name=well_name,
                                           model_components=model_components)


@app.task
def async_calculate_porosity_from_density(well_name,
                                          rhob_matrix: float = None,
                                          rhob_fluid: float = None,
                                          output_log_name: str = 'VSH_GR'):
    PorosityFromDensityNode.run_for_item(well_name=well_name,
                                         rhob_matrix=rhob_matrix,
                                         rhob_fluid=rhob_fluid,
                                         output_log_name=output_log_name)


@app.task
def async_calculate_shale_volume(well_name: str,
                                 algorithm: str = 'Linear',
                                 gr_matrix: float = None,
                                 gr_shale: float = None,
                                 output_log_name: str = 'VSH_GR'):
    if algorithm == 'ShaleVolumeLarionovOlderRockNode':
        node = ShaleVolumeLarionovOlderRockNode()
    elif algorithm == 'ShaleVolumeLarionovTertiaryRockNode':
        node = ShaleVolumeLarionovTertiaryRockNode()
    elif algorithm == 'ShaleVolumeLinearMethodNode':
        node = ShaleVolumeLinearMethodNode()
    else:
        raise ValueError(f"Unknown kind of algorithm: {algorithm}."
                         f"Acceptable values: 'ShaleVolumeLinearMethodNode', 'ShaleVolumeLarionovTertiaryRockNode', 'ShaleVolumeLarionovOlderRockNode'.")
    node.run_for_item(well_name, gr_matrix, gr_shale, output_log_name)


@app.task
def async_export_well_to_s3(destination: str,
                            wellname,
                            datasetname: str = 'LQC',
                            logs: Iterable[str] = None):
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
def async_log_reconstruction(well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs):
    LogReconstructionNode.run_for_item(well_names, log_families_to_train, log_family_to_predict, percent_of_wells_to_train, model_kwargs)


@app.task
def async_saturation_archie(well_name, a, m, n, rw, output_log_name):
    node = SaturationArchieNode()
    node.calculate_for_item(well_name, a, m, n, rw, output_log_name)
