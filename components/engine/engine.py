import logging

import celery_conf
from components.engine.workflow import Workflow
from utilities import my_timer


class Engine:
    """
    Class that launches EngineNodes
    """
    logger = logging.getLogger('Engine')

    def start(self, workflow: Workflow):
        self.logger.info(f"Starting calculation of workflow {workflow.name}")
        for step in workflow:
            self.logger.info(f'Starting {step}')
            node = step['node']()
            my_timer(node.run)(**step['parameters'])
            self.logger.info(f'Finished {step}')

        celery_conf.app.send_task('components.database.RedisStorage.build_log_meta_fields_index', ())
        celery_conf.app.send_task('components.database.RedisStorage.build_dataset_meta_fields_index', ())
        celery_conf.app.send_task('components.database.RedisStorage.build_well_meta_fields_index', ())


if __name__ == '__main__':
    workflow = Workflow('default')
    engine = Engine()
    my_timer(engine.start)(workflow)
