import sys

from components import ComponentGuiConstructor
from components.logger.widgets import QTextEditLogger
from components.mainwindow import GeoMainWindow

from PySide2.QtWidgets import QAction, QMenu, QTreeView, QHeaderView
from PySide2.QtCore import Qt

from components.projectree.ProjectTreeModel import ProjectTreeModel
from components.projectree.ProjectTreeEntry import ProjectEntryEnum


def initialize_component():
    gui = ProjectTreeGui()

    GeoMainWindow().addMenu(gui.toolBarActions())
    GeoMainWindow().addDockindWidget(gui.dockingWidget(), Qt.LeftDockWidgetArea)

    mod = sys.modules[__name__]
    mod.gui = gui


class ProjectTreeGui(ComponentGuiConstructor):

    def __init__(self):

        self._setupGui()
        self._connectSignals()

    def toolBarActions(self):
        menu = QMenu("ProjectTree")
        menu.addAction("Show project tree")

        return menu

    def dockingWidget(self):
        return self.tree_view

    def _setupGui(self):
        self.tree_view = QTreeView()
        self.tree_view.setWindowTitle("Project Tree")
        self.tree_view.setAlternatingRowColors(True)

        model = ProjectTreeModel()
        self.tree_view.setModel(model)
        # self.tree_view.expandAll()


        # self.tree_view.setHeaderHidden(True)

        header_view = self.tree_view.header()
        header_view.setSectionResizeMode(ProjectEntryEnum.NAME.value,
                                         QHeaderView.ResizeToContents)

    def _connectSignals(self):
        pass
