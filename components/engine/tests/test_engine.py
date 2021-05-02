import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine import Engine
from components.importexport.las import import_to_db
from tasks import async_get_basic_log_stats, async_run_workflow

PATH_TO_TEST_DATA = os.path.join('test_data')


class TestEngine(unittest.TestCase):
    def setUp(self) -> None:
        # pass
        self._s = RedisStorage()
        self._s.flush_db()

        for filename in os.listdir(PATH_TO_TEST_DATA):
            import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

        p = Project()
        for wellname in p.list_wells().keys():
            async_get_basic_log_stats(wellname)
            w = Well(wellname)
            for datasetname in w.datasets:
                log_id = 'GR'
                ds = WellDataset(w, datasetname)
                log = BasicLog(ds.id, log_id)
                log.meta.family = 'Gamma Ray'
                log.save()

    def test_engine_runs_with_no_exceptions(self):
        engine = Engine()
        engine.start()

class TestEngineInCelery(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()

        for filename in os.listdir(PATH_TO_TEST_DATA):
            import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))


    def test_engine_running_in_celery(self):
        async_run_workflow.delay()