import os
from typing import Iterable

from celery import Celery
from celery.result import AsyncResult

from settings import REDIS_HOST

REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_PORT', 0)
app = Celery('tasks', broker=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
app.conf.result_backend = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
app.conf.result_expires = 60*60*24
app.conf.ack_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_serializer = 'pickle'
app.conf.task_compression = 'gzip'
app.conf.accept_content = ['pickle', 'json']


def get_running_tasks():
    i = app.control.inspect()
    return i.active()


def get_pending_tasks():
    i = app.control.inspect()
    return i.reserved()


def check_task_completed(asyncresult: AsyncResult) -> bool:
    return asyncresult.status in ['SUCCESS', 'FAILURE']


def check_task_successful(asyncresult: AsyncResult) -> bool:
    return asyncresult.successful()


def wait_till_completes(results: Iterable[AsyncResult]) -> None:
    while not all(map(check_task_completed, results)):
        continue
