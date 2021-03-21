import logging

from PySide2.QtCore import QAbstractItemModel, QModelIndex
from PySide2.QtCore import Qt
from PySide2.QtGui import QIcon, QPalette

from components.projectree.ProjectTreeEntry import ProjectEntryEnum
from components.projectree.WellEntries import WellManagerEntry
# from components.projectree.TabletEntries import TabletTemplateManagerEntry

from components.domain.Well import Well


gamma_logger = logging.getLogger('gamma_logger')


class ProjectTreeModel(QAbstractItemModel):

    def __init__(self):
        QAbstractItemModel.__init__(self)

        self.entries = [WellManagerEntry(model = self),
                        # TabletTemplateManagerEntry(model = self),
                        ]

    def columnCount(self, index: QModelIndex):
        return len(ProjectEntryEnum)

    def rowCount(self, parent: QModelIndex):
        if not parent.isValid():
            return len(self.entries)

        tree_entry = parent.internalPointer()
        return len(tree_entry.entries)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return ['Name', 'Value'][section]

        return super().headerData(section, orientation, role)

    def index(self, row, column, parent: QModelIndex):
        if not parent.isValid():
            return self.createIndex(row, column, self.entries[row])

        entry = parent.internalPointer()

        if len(entry.entries) == 0:
            return QModelIndex()

        return self.createIndex(row, column, entry.entries[row])

    def parent(self, index: QModelIndex):
        entry = index.internalPointer()

        parent_entry = entry.parent()

        if parent_entry is None:
            return QModelIndex()

        parent_parent_entry = parent_entry.parent()

        position = 0

        if parent_parent_entry is None:
            position = self._getEntryPosition(parent_entry)
        else:
            parent_parent_entry.positionOfEntry(parent_entry)

        return self.createIndex(position, 0, parent_entry)

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        return index.internalPointer().data(role, index.column())

    def setData(self, index: QModelIndex, value, role=None):
        return False

    def flags(self, index: QModelIndex):
        flags = super().flags(index)
        # flags ^= Qt.ItemIsEditable
        # flags ^= Qt.ItemIsSelectable

        return flags

    def _getEntryPosition(self, entry):
        return self.entries.index(entry)

    def onClicked(self, index: QModelIndex):
        pass
