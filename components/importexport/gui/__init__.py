# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.
import logging
import multiprocessing as mp
import sys
import time

import celery
from PySide2.QtCore import Qt
from PySide2.QtWidgets import QMenu, QFileDialog, QProgressDialog

from celery_conf import wait_till_completes
from components.database.RedisStorage import RedisStorage
from components import ComponentGuiConstructor
from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton
from components.importexport.markers_importexport import import_markers_csv
from components.importexport.well_heads import well_heads_csv_header, import_well_heads_csv
from components.importexport.las.async_las_import import compress_and_send_for_parsing
from components.mainwindow.gui import GeoMainWindow

gamma_logger = logging.getLogger('gamma_logger')


class ImportExportGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu = QMenu("Import Export")
        import_action = menu.addAction("Import LAS files")
        import_wellheads_action = menu.addAction("Import Wellheads")
        import_markers_action = menu.addAction("Import Markers")

        import_action.triggered.connect(self._showImportWidget)
        import_wellheads_action.triggered.connect(self._showImportWellHeadsWidget)
        import_markers_action.triggered.connect(self._showImportMarkersWidget)

        return menu

    def dockingWidget(self):
        pass

    def _showImportWidget(self):
        files, _ = QFileDialog.getOpenFileNames(GeoMainWindow(),
                                                'Select one or more files to import',
                                                None,
                                                'LAS Files (*.las)')
        if not files:
            return

        s = RedisStorage()
        if not s.common_data_loaded():
            from load_common_data import load_common_data
            load_common_data()

        gamma_logger.info('Files: {}'.format(len(files)))

        progress = QProgressDialog("Parsing LAS files...", None, 0, len(files), GeoMainWindow())
        progress.setWindowModality(Qt.WindowModal)

        # Parallel process-based parsing
        start = time.time()
        async_results = []
        with mp.Pool() as pool:
            for i, result in enumerate(pool.imap_unordered(compress_and_send_for_parsing, files)):
                progress.setValue(i + 1)
                async_results.append(result)

        gamma_logger.info(f'Sent all files to queue: {time.time() - start}')

        wait_till_completes(async_results)

        end = time.time()
        gamma_logger.info('All LAS files loaded. Elapsed: {}'.format(end - start))
        gamma_logger.info('Seconds per file: {}'.format((end - start) / (len(files) or 1)))

        # run engine after import completes
        gamma_logger.info("Sending task to engine")
        workflow_task = celery.current_app.send_task('tasks.async_run_workflow', ())
        # wait_till_completes((workflow_task, ))

        DbEventDispatcherSingleton().wellsAdded.emit()

    def _showImportWellHeadsWidget(self):
        file, _ = QFileDialog.getOpenFileName(GeoMainWindow(),
                                              'Select one file to import',
                                              None,
                                              'CSV Files (*.csv)')
        if not file:
            return

        with open(file, 'r') as f:
            header = well_heads_csv_header(f, delimiter=';')
            import_well_heads_csv(f, header, delimiter=';')

        DbEventDispatcherSingleton().wellsAdded.emit()

    def _showImportMarkersWidget(self):
        file, _ = QFileDialog.getOpenFileName(GeoMainWindow(),
                                              'Select one file to import',
                                              None,
                                              'CSV Files (*.csv)')
        if not file:
            return

        with open(file, 'r') as f:
            import_markers_csv(f)

        DbEventDispatcherSingleton().wellsAdded.emit()


def initialize_component():
    gui = ImportExportGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


if 'unittest' not in sys.modules.keys():
    initialize_component()
