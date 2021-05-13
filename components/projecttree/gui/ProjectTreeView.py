from PySide2.QtWidgets import (QAction,
                               QMenu,
                               QTreeView,
                               QHeaderView,
                               QAbstractItemView)

from PySide2.QtCore import Qt, QPoint

class ProjectTreeView(QTreeView):

    def __init__(self, parent=None):
        QTreeView.__init__(self, parent)

        self.setWindowTitle("Project Tree")
        self.setAlternatingRowColors(True)
        self.setDragEnabled(True)
        self.setDragDropMode(QAbstractItemView.DragOnly)
        self.setDropIndicatorShown(True)
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.setContextMenuPolicy(Qt.CustomContextMenu)

        self.customContextMenuRequested.connect(self.on_context_menu)


    def on_context_menu(self, point: QPoint):

        modelIndex = self.indexAt(point)

        if not modelIndex.isValid():
            return

        treeEntry = modelIndex.internalPointer()
        menu = treeEntry.contextMenu()

        if not menu is None:
            menu.exec_(self.mapToGlobal(point))
