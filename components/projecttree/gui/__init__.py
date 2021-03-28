import sys

from PySide2.QtWidgets import QAction, QMenu, QTreeView, QHeaderView, QAbstractItemView
from PySide2.QtCore import Qt

from components import ComponentGuiConstructor
from components.mainwindow.gui import GeoMainWindow

from components.projecttree.gui.ProjectTreeModel import ProjectTreeModel
from components.projecttree.gui.ProjectTreeEntry import ProjectEntryEnum


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
        self.tree_view.setDragEnabled(True)
        self.tree_view.setDragDropMode(QAbstractItemView.DragOnly)
        self.tree_view.setDropIndicatorShown(True)
        self.tree_view.setSelectionMode(QAbstractItemView.ExtendedSelection)

        model = ProjectTreeModel()
        self.tree_view.setModel(model)
        # self.tree_view.expandAll()
        # self.tree_view.setHeaderHidden(True)

        header_view = self.tree_view.header()
        header_view.setSectionResizeMode(ProjectEntryEnum.NAME.value,
                                         QHeaderView.ResizeToContents)

    def _connectSignals(self):
        pass


def initialize_component():
    gui = ProjectTreeGui()

    GeoMainWindow().addMenu(gui.toolBarActions())
    GeoMainWindow().addDockindWidget(gui.dockingWidget(), Qt.LeftDockWidgetArea)

    mod = sys.modules[__name__]
    mod.gui = gui

if not 'unittest' in sys.modules.keys():
    initialize_component()
