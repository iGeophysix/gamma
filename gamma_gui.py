#!/usr/bin/env python3

import sys
import logging

import os
from PySide2.QtCore import QCoreApplication, Qt
from PySide2.QtWidgets import QApplication
from celery import Celery
from settings import REDIS_HOST

import components

REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_PORT', 0)
app = Celery('tasks', broker=f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
app.conf.result_backend = f'redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}'
app.conf.result_expires = 60

def main(profile = False):
    QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

    app = QApplication(sys.argv)

    font = app.font()
    font.setPointSize(9)
    app.setFont(font)


    components.initialize_components()

    gamma_logger = logging.getLogger('gamma_logger')

    if profile:
        app.exec_()
    else:
        sys.exit(app.exec_())


if __name__ == "__main__":
    main()
