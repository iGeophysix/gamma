import os

from celery import Celery
from celery.result import AsyncResult
from settings import REDIS_HOST

REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_PORT', 0)
app = Celery('tasks', broker=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
app.conf.result_backend = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
app.conf.result_expires = 60
app.conf.task_serializer = 'pickle'
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
