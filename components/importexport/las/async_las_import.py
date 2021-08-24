import os

from celery_conf import app as celery_app


def compress_and_send_for_parsing(filename):
    with open(filename, 'rb') as f:
        las_data = f.read()
    result = celery_app.send_task('tasks.async_read_las', kwargs={'filename': filename, 'las_data': las_data})
    return result
