import sys

from PySide2.QtWidgets import QMenu

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow
from load_common_data import load_common_data


class DomainGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu = QMenu("Project")
        tablet_action = menu.addAction("Load Common")
        tablet_action.triggered.connect(load_common_data)
        return menu

    def dockingWidget(self):
        pass


def initialize_component():
    gui = DomainGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


if 'unittest' not in sys.modules.keys():
    initialize_component()
