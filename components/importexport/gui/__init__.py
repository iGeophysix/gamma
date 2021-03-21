# Copyright (C) 2019 by Dmitry Pinaev <dimitry.pinaev@gmail.com>
# All rights reserved.

import logging
import os
import sys
import time

import multiprocessing as mp

from PySide2.QtWidgets import QAction, QMenu, QFileDialog, QProgressDialog
from PySide2.QtCore import Qt

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow

from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton
from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport import las

gamma_logger = logging.getLogger('gamma_logger')


class ImportExportGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu= QMenu("Import Export")
        import_action = menu.addAction("Import")

        import_action.triggered.connect(self._showImportWidget)

        return menu

    def dockingWidget(self):
        pass

    def _showImportWidget(self):
        root_directory = os.path.dirname(sys.modules['__main__'].__file__)
        files, _= QFileDialog.getOpenFileNames(GeoMainWindow(),
                                               'Select one or mor files to import',
                                               root_directory,
                                               'LAS Files (*.las)')

        start = time.time()

        pool = mp.Pool(mp.cpu_count())


        gamma_logger.info('CPU Count: {}'.format(mp.cpu_count()))
        gamma_logger.info('Files: {}'.format(len(files)))

        las_file_structures = []

        progress = QProgressDialog("Parsing LAS files...", "Abort", 0, len(files), GeoMainWindow())
        progress.setWindowModality(Qt.WindowModal)

        # Parallel Process-based parsing
        for (i, las_file) in enumerate(pool.imap_unordered(las.parse, files)):
            progress.setValue(i)

            if las_file.error_message:
                gamma_logger.error(las_file.error_message)
            else:
                las_file_structures.append(las_file)

            if progress.wasCanceled():
                print('Canceled')
                return;

        progress.setValue(len(files))

        pool.close()
        pool.join()

        # Serial Parsing
        # for (i, f) in enumerate(files):
            # progress.setValue(i)
            # las_file_structures.append(las.parse_las_file(f))

        # progress.setValue(len(files))


        end = time.time()
        gamma_logger.info('Elapsed: {}'.format(end - start))
        gamma_logger.info('Seconds per file: {}'.format((end - start)/(len(files) or 1)))

        if las_file_structures:
            # import to db here
            for las_struct in las_file_structures:
                gamma_logger.info(f'Importing file "{las_struct.filename}".')
                if las_struct.error_message:
                    gamma_logger.error(f'File "{las_struct.filename}" has an error "{las_struct.error_message}"')

                las.import_to_db(las_structure=las_struct)
        else:
            gamma_logger.debug('No LAS files were chosen.')

        DbEventDispatcherSingleton().wellsAdded.emit()


def initialize_component():
    gui = ImportExportGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui

if not 'unittest' in sys.modules.keys():
    initialize_component()
