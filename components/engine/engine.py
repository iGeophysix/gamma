import logging

from components.engine.engine_node import EngineProgress
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
        engine_progress = EngineProgress()
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


if __name__ == '__main__':
    workflow = Workflow('default')
    engine = Engine()
    my_timer(engine.start)(workflow)
