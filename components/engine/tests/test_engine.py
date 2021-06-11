import os
import unittest

from components.database.RedisStorage import RedisStorage
from components.engine.engine import Engine
from components.engine.workflow import Workflow
from components.importexport.las import import_to_db
from settings import BASE_DIR
from tasks import async_run_workflow

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data')


class TestEngine(unittest.TestCase):
    def setUp(self) -> None:
        # pass
        self._s = RedisStorage()
        self._s.flush_db()
        files = ['7_1-2 S.las',
                 '15_9-13.las', ]
        workflow = Workflow('test_workflow')
        workflow.set_steps([{"node": "LogResolutionNode", "parameters": {}}])

        for filename in files:
            if filename.endswith(".las"):
                import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

    def test_engine_runs_with_no_exceptions(self):
        workflow = Workflow('test_workflow')
        engine = Engine()
        engine.start(workflow)

    def test_engine_running_in_celery(self):
        async_run_workflow('test_workflow')
