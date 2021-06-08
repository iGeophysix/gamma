import logging
import sys

from PySide2.QtWidgets import QMenu

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


if not 'unittest' in sys.modules.keys():
    initialize_component()
