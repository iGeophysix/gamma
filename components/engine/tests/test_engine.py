import datetime
import os
import time
import unittest

import celery_conf
from components.database.RedisStorage import RedisStorage
from components.engine.engine import Engine
from components.engine.workflow import Workflow
from components.importexport.las import import_to_db
from settings import BASE_DIR
from tasks import async_run_workflow

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data')


class TestEngine(unittest.TestCase):
    def setUp(self) -> None:
        self._s = RedisStorage()
        self._s.flush_db()
        files = ['7_1-2 S.las',
                 '15_9-13.las', ]
        workflow = Workflow('test_workflow')
        workflow.set_steps([
            {"node": "LogResolutionNode", "parameters": {}},
        ])
        workflow.save()
        for filename in files:
            if filename.endswith(".las"):
                import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, filename))

    def test_engine_runs_with_no_exceptions(self):
        workflow = Workflow('test_workflow')
        engine = Engine()
        engine.start(task_id='test', workflow=workflow)

    def test_engine_running_in_celery(self):
        async_run_workflow.delay('test_workflow')

    def test_engine_can_run_one_workflow_at_a_time(self):
        """
        Launch workflow three times. First two should abort and the third should finish successfully.
        :return:
        """

        first = async_run_workflow.delay('default')
        second = async_run_workflow.delay('default')
        time.sleep(1)
        third = async_run_workflow.delay('test_workflow')

        celery_conf.wait_till_completes((first, second, third,))

        self.assertTrue((first.status == 'REVOKED') or not first.get()['finished'])
        self.assertTrue((second.status == 'REVOKED') or not second.get()['finished'])
        self.assertTrue((third.status == 'SUCCESS') and third.get()['finished'])

