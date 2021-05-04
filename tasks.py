import gzip

from celery_conf import app
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine import Engine
from components.importexport import las
from components.importexport.FamilyAssigner import FamilyAssigner
from components.importexport.las import import_to_db
from components.petrophysics.curve_operations import get_basic_curve_statistics, get_log_resolution, rescale_curve
from components.petrophysics.log_splicing import splice_logs
from components.petrophysics.run_detection import detect_runs_in_well


@app.task
def async_run_workflow(workflow_id: str = None):
    workflow = Engine()
    workflow.start()


@app.task
def async_read_las(wellname: str = None, datasetname: str = None, filename: str = None, las_data: str = None):
    if filename is not None and las_data is None:
        well = Well(wellname)
        dataset = WellDataset(well, datasetname)
        import_to_db(filename=filename, well=well, well_dataset=dataset)
    elif las_data is not None:
        las_structure = las.parse(filename=filename, data=gzip.decompress(las_data))
        well = Well(wellname) if wellname is not None else None
        dataset = WellDataset(well, datasetname) if datasetname is not None else None
        import_to_db(las_structure=las_structure, well=well, well_dataset=dataset)
    else:
        raise Exception('Incorrect function call')


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
    w = Well(wellname)
    datasetnames = w.datasets if datasetnames is None else datasetnames

    # get all data from specified well and datasets
    for dataset_name in datasetnames:
        d = WellDataset(w, dataset_name)
        log_names = d.log_list if logs is None else logs
        for log_name in log_names:
            log = BasicLog(d.id, log_name)
            log_resolution = get_log_resolution(log.values, log.meta)
            log.meta.log_resolution = {'value': log_resolution}
            log.save()


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
def async_recognize_family(wellname: str, datasetnames: list[str] = None) -> None:
    """
    Recognize log family in well datasets
    :param wellname:
    :param datasetnames:
    :return:
    """
    fa = FamilyAssigner()
    w = Well(wellname)
    if datasetnames is None:
        datasetnames = w.datasets

    for datasetname in datasetnames:
        wd = WellDataset(w, datasetname)

        new_metadata = {log: {} for log in wd.log_list}
        for log in new_metadata.keys():
            '''(log family, unit class, detection reliability)'''
            result = fa.assign_family(log)
            family, unit_class, reliability = None, None, None if result is None else result
            l = BasicLog(wd.id, log)
            l.meta |= {
                "family": family,
                "unit_class": unit_class,
                "family_detection_reliability": reliability
            }
            l.save()


@app.task
def async_splice_logs(wellname: str, datasetnames: list[str] = None, logs: list[str] = None, output_dataset_name: str = 'Spliced') -> None:
    """
    Async method to splice logs. Takes  logs in datasets and outputs it into a specified output dataset
    :param wellname: Well name as string
    :param datasetnames: Datasets' name as list of strings. If None then uses all datasets
    :param logs: Logs' names as list of string. If None then uses all logs available in datasets
    :param output_dataset_name: Name of output dataset
    """
    w = Well(wellname)
    logs_data, logs_meta = splice_logs(w, datasetnames, logs)
    wd = WellDataset(w, output_dataset_name, new=True)
    for log_name in logs_data.keys():
        log = BasicLog(wd.id, log_name)
        log.values = logs_data[log_name]
        log.meta = logs_meta[log_name]
        log.save()
