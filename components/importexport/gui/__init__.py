# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.
import gzip
import logging
import multiprocessing as mp
import os
import sys
import time

import celery
from PySide2.QtCore import Qt
from PySide2.QtWidgets import QMenu, QFileDialog, QProgressDialog

from celery_conf import app as celery_app, check_task_completed
from components import ComponentGuiConstructor
from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton
from components.mainwindow.gui import GeoMainWindow

gamma_logger = logging.getLogger('gamma_logger')


def compress_and_send_for_parsing(filename):
    gamma_logger.info(f"PID {os.getpid()}: Reading {filename}")
    with open(filename, 'rb') as f:
        las_data = gzip.compress(f.read())
    result = celery_app.send_task('tasks.async_read_las', kwargs={'filename': filename, 'las_data': las_data})
    return result


class ImportExportGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu = QMenu("Import Export")
        import_action = menu.addAction("Import")

        import_action.triggered.connect(self._showImportWidget)

        return menu

    def dockingWidget(self):
        pass

    def _showImportWidget(self):
        root_directory = os.path.dirname(sys.modules['__main__'].__file__)
        files, _ = QFileDialog.getOpenFileNames(GeoMainWindow(),
                                                'Select one or mor files to import',
                                                root_directory,
                                                'LAS Files (*.las)')

        start = time.time()

        pool = mp.Pool(mp.cpu_count())

        # gamma_logger.info('CPU Count: {}'.format(mp.cpu_count()))
        gamma_logger.info('Files: {}'.format(len(files)))

        progress = QProgressDialog("Parsing LAS files...", "Abort", 0, len(files), GeoMainWindow())
        progress.setWindowModality(Qt.WindowModal)

        # Parallel Process-based parsing
        async_results = []
        for file in files:
            async_results.append(compress_and_send_for_parsing(file)) # TODO: make it parallel

        # for (i, result) in enumerate(pool.imap_unordered(compress_and_send_for_parsing, files)):
        #     progress.setValue(i)
        #     async_results.append(result)
        pool.close()
        pool.join()

        progress.setValue(len(files))

        while not all(map(check_task_completed, async_results)):
            continue

        end = time.time()
        gamma_logger.info('Elapsed: {}'.format(end - start))
        gamma_logger.info('Seconds per file: {}'.format((end - start) / (len(files) or 1)))

        # run engine after import completes
        gamma_logger.info("Sending task to engine")
        celery.current_app.send_task('tasks.async_run_workflow', ())

        DbEventDispatcherSingleton().wellsAdded.emit()


def initialize_component():
    gui = ImportExportGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


if not 'unittest' in sys.modules.keys():
    initialize_component()
