import sys
import logging

from PySide2.QtWidgets import QAction, QMenu, QFileDialog, QProgressDialog
from PySide2.QtCore import Qt

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow

from components.tablet.gui.TabletWidget import TabletWidget

gamma_logger = logging.getLogger("gamma_logger")


class TabletGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu = QMenu("Visualization")
        tablet_action = menu.addAction("Tablet")
        tablet_action.triggered.connect(self._showTableWidget)

        return menu

    def dockingWidget(self):
        pass

    def _showTableWidget(self):

        GeoMainWindow().addToCentral(TabletWidget(), "Tablet")


def initialize_component():
    gui = TabletGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


print("IMPORT AND INIT")

if not 'unittest' in sys.modules.keys():
    initialize_component()
