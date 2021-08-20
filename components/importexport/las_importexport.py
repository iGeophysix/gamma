import datetime
import os
import re
from typing import Iterable

import boto3

from celery_conf import app as celery_app
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode
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
    def name(cls):
        return cls.__name__

    @classmethod
    def version(cls):
        return 0

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
        filename = re.sub('[^-a-zA-Z0-9_.() ]+', '_', f'{wellname}_{datasetname}.las')
        las.write(filename, version=2)
        cls.s3.upload_file(filename, 'public', f'{destination}/{filename}')
        os.remove(filename)

    @classmethod
    def run(cls, **kwargs):
        """
        :param destination: Name of output folder in public bucket of S3.
        """
        destination = kwargs.get('destination', None)
        async_job = kwargs.get('async_job', True)
        p = Project()
        if destination is None:
            destination = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        else:
            # clear destination folder
            response = cls.s3.list_objects(Bucket='public', Prefix=f'{destination}')
            if 'Contents' in response.keys():
                keys = [k['Key'] for k in response['Contents']]
                for key in keys:
                    cls.s3.delete_object(Bucket='public', Key=key)
        if async_job:
            tasks = []
            for well_name in p.list_wells():
                tasks.append(celery_app.send_task('tasks.async_export_well_to_s3', kwargs={'destination': destination, 'wellname': well_name,
                                                                                           'datasetname': 'LQC', 'logs': None}))
            cls.track_progress(tasks)
        else:
            for well_name in p.list_wells():
                cls.export_well_dataset(destination, well_name, datasetname='LQC', logs=None)


if __name__ == '__main__':
    node = LasExportNode()
    node.run(destination='LQC', async_job=False)
