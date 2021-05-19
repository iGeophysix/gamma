import os
from typing import Iterable

from celery import Celery
from celery.result import AsyncResult

# CELERY CONFIG
QUEUE_HOST = os.environ.get('CELERY_REDIS_HOST', '127.0.0.1')
QUEUE_PORT = os.environ.get('CELERY_REDIS_PORT', 6379)
QUEUE_DB = os.environ.get('CELERY_QUEUE_DB', 0)
CELERY_RESULTS_DB = os.environ.get('RESULTS_DB', 2)

app = Celery('tasks', broker=f'redis://{QUEUE_HOST}:{QUEUE_PORT}/{QUEUE_DB}')
app.conf.result_backend = f'redis://{QUEUE_HOST}:{QUEUE_PORT}/{CELERY_RESULTS_DB}'
app.conf.result_expires = 60 * 60  # in seconds
# app.conf.ack_late = True
# app.conf.worker_prefetch_multiplier = 2
app.conf.task_serializer = 'pickle'
# app.conf.task_compression = 'gzip'
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
