import os
from typing import Iterable

import boto3

from celery_conf import wait_till_completes, app as celery_app
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode
from components.importexport.las import import_to_db
from components.importexport.las.las_export import create_las_file
from settings import MINIO_HOST, MINIO_PORT, MINIO_USER, MINIO_PASSWORD


class LasImportNode(EngineNode):
    """
    Engine node to import files
    """

    def run(self, paths: Iterable[str]):
        """
        :param paths: Absolute paths to files, accessible to task reader
        """
        for path in paths:
            import_to_db(filename=path)


class LasExportNode(EngineNode):
    """
    Engine node to export output logs to a las file.
    Saves LQC datasets to one las per well. Puts it into a single archive
    and stores to an S3
    """
    s3_session = boto3.session.Session()
    s3 = s3_session.client(
        service_name='s3',
        endpoint_url=f'http://{MINIO_HOST}:{MINIO_PORT}/',
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )

    @classmethod
    def export_well_dataset(cls, destination: str, wellname, datasetname: str = 'LQC', logs: Iterable[str] = None):
        """
        Method to assemble a las file in one well and one dataset to push it to public bucket in S3
        :param destination: name of folder to store data to
        :param wellname: well name to export
        :param datasetname: dataset name to export. Default: 'LQC'
        :param logs: logs names to export. Default: None - export all logs in the dataset
        """
        well = Well(wellname)
        dataset = WellDataset(well, datasetname)
        logs_to_export = dataset.log_list if logs is None else logs
        if not all(map(lambda x: x in dataset.log_list, logs_to_export)):
            raise KeyError(f'Not all specified logs were found in well {wellname} dataset {datasetname}: {logs_to_export}')
        paths = [(datasetname, logname) for logname in logs_to_export]
        las = create_las_file(well_name=wellname, paths_to_logs=paths)
        las.write(f'{wellname}_{datasetname}.las', version=2)
        cls.s3.upload_file(f'{wellname}_{datasetname}.las', 'public', f'{destination}/{wellname}_{datasetname}.las')
        os.remove(f'{wellname}_{datasetname}.las')

    def run(self, destination: str = 'Export', async_job: bool = True):
        """
        :param destination: Name of output folder in public bucket of S3
        """
        p = Project()
        if async_job:
            tasks = []
            for well_name in p.list_wells():
                tasks.append(celery_app.send_task('tasks.async_export_well_to_s3', kwargs={'destination': destination, 'wellname': well_name,
                                                                                           'datasetname': 'LQC', 'logs': None}))
            wait_till_completes(tasks)
        else:
            for well_name in p.list_wells():
                self.export_well_dataset(destination, well_name, datasetname='LQC', logs=None)


if __name__ == '__main__':
    node = LasExportNode()
    node.run(async_job=False)
