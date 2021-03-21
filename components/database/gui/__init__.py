import json
import sys

from PySide2.QtWidgets import QAction, QMenu

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow


class DatabaseGui(ComponentGuiConstructor):

    def toolBarActions(self):
        pass

    def dockingWidget(self):
        pass

def initialize_component():

    # loadDefaultPropertiesFromFile()
    # loadDefaultUnitsFromFile()
    # loadDefaultCurveFamiliesFromFile()
    # loadDefaultTabletTEemplatesFromFile()

    gui = DatabaseGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


if not 'unittest' in sys.modules.keys():
    initialize_component()
