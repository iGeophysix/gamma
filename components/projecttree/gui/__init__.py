import sys

from PySide2.QtCore import Qt
from PySide2.QtWidgets import (QAction, QMenu, QHeaderView)

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow

from components.projecttree.gui.ProjectTreeEntry import ProjectEntryEnum
from components.projecttree.gui.ProjectTreeModel import ProjectTreeModel
from components.projecttree.gui.ProjectTreeView import ProjectTreeView


class ProjectTreeGui(ComponentGuiConstructor):

    def __init__(self):

        self._setupGui()

    def toolBarActions(self):
        menu = QMenu("ProjectTree")
        self._show_project_tree_action = menu.addAction("Show project tree")
        self._clear_database = menu.addAction("Clear Database")
        self._refresh_database = menu.addAction("Refresh Database")

        self._connectSignals()

        return menu

    def dockingWidget(self):
        return self.tree_view

    def _setupGui(self):
        self.tree_view = ProjectTreeView()




        self.model = ProjectTreeModel()
        self.tree_view.setModel(self.model)

        header_view = self.tree_view.header()
        header_view.setSectionResizeMode(ProjectEntryEnum.NAME.value,
                                         QHeaderView.ResizeToContents)

        # self.tree_view.expandAll()
        # self.tree_view.setHeaderHidden(True)


    def _connectSignals(self):
        self._clear_database.triggered.connect(self.model.on_clear_database)
        self._refresh_database.triggered.connect(self.model.on_refresh_database)


def initialize_component():
    gui = ProjectTreeGui()

    GeoMainWindow().addMenu(gui.toolBarActions())
    GeoMainWindow().addDockindWidget(gui.dockingWidget(), Qt.LeftDockWidgetArea)

    mod = sys.modules[__name__]
    mod.gui = gui

if not 'unittest' in sys.modules.keys():
    initialize_component()
