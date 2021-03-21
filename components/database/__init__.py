import json
import sys

from PySide2.QtWidgets import QAction, QMenu

from components import ComponentGuiConstructor
from components.mainwindow import GeoMainWindow


def initialize_component():

    # loadDefaultPropertiesFromFile()
    # loadDefaultUnitsFromFile()
    # loadDefaultCurveFamiliesFromFile()
    # loadDefaultTabletTEemplatesFromFile()

    gui = DatabaseGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


class DatabaseGui(ComponentGuiConstructor):

    def toolBarActions(self):
        pass

    def dockingWidget(self):
        pass
