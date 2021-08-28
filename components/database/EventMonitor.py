import logging
import json

from components.database.RedisStorage import RedisStorage, REDIS_DB, ENGINE_RUNS_TABLE_NAME
from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton


class RedisEventMonitor:
    '''
    Catch and process Redis events
    '''
    _logger = logging.getLogger('gamma_logger')
    _redis = RedisStorage()
    _engine_runs = {
        'channel': f'__keyspace@{REDIS_DB}__:{ENGINE_RUNS_TABLE_NAME}',
        'node': None,
        'progress': None
    }

    def __init__(self):
        pubsub = self._redis.connection.pubsub()
        pubsub.subscribe(**{self._engine_runs['channel']: self._event_handler})
        self._thread = pubsub.run_in_thread(sleep_time=0.01)

    @staticmethod
    def _event_handler(msg):
        '''
        Callback handler for Redis event messages
        '''
        if msg['type'] == 'message':
            engine_runs = RedisEventMonitor._engine_runs
            channel = msg['channel'].decode()
            if channel == engine_runs['channel']:
                # cmd = msg['data'].decode()
                if RedisEventMonitor._redis.object_exists(ENGINE_RUNS_TABLE_NAME):
                    status = json.loads(RedisEventMonitor._redis.object_get(ENGINE_RUNS_TABLE_NAME))
                    if status:
                        current_node_name, current_node_info = tuple(status.items())[-1]
                        progress = int(current_node_info['completion'] * 100)
                        if current_node_name != engine_runs['node'] or progress != engine_runs['progress']:
                            engine_runs['node'] = current_node_name
                            engine_runs['progress'] = progress
                            progress_msg = f'{current_node_name}: {progress}%'
                            RedisEventMonitor._logger.info(progress_msg)

                            if current_node_name == 'VolumetricModelSolverNode' and progress == 100:
                                DbEventDispatcherSingleton().wellsAdded.emit()

    def stop(self):
        '''
        Kill monitor thread. Do it befor main app exit
        '''
        self._thread.stop()

    def __del__(self):
        self.stop()
