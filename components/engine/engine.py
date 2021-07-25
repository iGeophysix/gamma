import logging

from components.database.RedisStorage import RedisStorage
from components.engine.workflow import Workflow
from utilities import my_timer


class Engine:
    """
    Class that launches EngineNodes
    """
    logger = logging.getLogger('Engine')

    def start(self, task_id: str, workflow: Workflow):
        result = {
            "finished": False,
            "status_text": '',
            "nodes": [],
            "steps": {
                'completed': 0,
                'total': len(workflow)
            }
        }
        engine_progress = EngineProgress(task_id)
        try:
            self.logger.info(f"Starting calculation of workflow {workflow.name} ({len(workflow)} steps)")
            for step in workflow:
                self.logger.info(f'Starting {step}')
                node = step['node']()
                result['nodes'].append(
                    {'node': step['node'].name()}
                )

                step['parameters']['engine_progress'] = engine_progress
                my_timer(node.run)(**step['parameters'])
                self.logger.info(f'Finished {step}')
                result['steps']['completed'] += 1
        except Exception as exc:
            result['status_text'] = repr(exc)
        else:
            result['finished'] = True
        return result


class EngineProgress:
    def __init__(self, task_id):
        self._task_id = task_id
        self._nodes = {}

    def save(self):
        s = RedisStorage()
        s.table_key_set('engine_runs', self._task_id, self._nodes)

    def update(self, node_name, node_status):
        '''
        Update nodes status
        :param node_name:
        :param node_status:
        :return:
        '''
        self._nodes[node_name] = node_status
        self.save()


if __name__ == '__main__':
    workflow = Workflow('default')
    engine = Engine()
    my_timer(engine.start)(workflow)
