import json
import logging

from PySide2.QtCore import QAbstractItemModel, QModelIndex, QMimeData
from PySide2.QtCore import Qt
from PySide2.QtGui import QIcon, QPalette

from components.projecttree.gui.ProjectTreeEntry import ProjectEntryEnum
from components.projecttree.gui.WellEntries import WellEntry, WellDatasetEntry, CurveEntry
# from components.projecttree.TabletEntries import TabletTemplateManagerEntry
from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton

from components.database.RedisStorage import RedisStorage
from components.domain.Well import Well


gamma_logger = logging.getLogger('gamma_logger')


class ProjectTreeModel(QAbstractItemModel):

    def __init__(self):
        QAbstractItemModel.__init__(self)

        self._s = RedisStorage()
        self._loadWells()

        DbEventDispatcherSingleton().wellsAdded.connect(self.onWellsAdded)

    def on_clear_database(self):
        self.beginResetModel()
        self._s.flush_db()
        self._loadWells()
        self.endResetModel()

    def on_refresh_database(self):
        self.beginResetModel()
        self._loadWells()
        self.endResetModel()

    def _loadWells(self):
        self.entries = []
        well_names = self._s.list_wells()
        self.entries = [WellEntry(model=self,
                                  parent=None,
                                  well_name=well_name) \
                        for well_name in well_names]


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

    def mimeData(self, indexes):
        # Just indexes from the 0-th column
        indexes = [index for index in indexes if index.column() == 0]

        data = []

        for index in indexes:
            if isinstance(index.internalPointer(), WellEntry):
                wellEntry = index.internalPointer()
                for datasetEntry in wellEntry.entries:
                    for curveEntry in datasetEntry.entries:
                        data.append((wellEntry.data(), datasetEntry.data(), curveEntry.data()))
            if isinstance(index.internalPointer(), CurveEntry):
                curveEntry = index.internalPointer()
                datasetEntry = curveEntry.parent()
                wellEntry = datasetEntry.parent()
                data.append((wellEntry.data(), datasetEntry.data(), curveEntry.data()))

        data_rebuilt = {}

        for (w, d, c) in data:
            if w not in data_rebuilt:
                data_rebuilt[w] = {}

            if not d in data_rebuilt[w]:
                data_rebuilt[w][d] = []

            data_rebuilt[w][d].append(c)


        mime = QMimeData()
        mime.setText(json.dumps(data_rebuilt))

        return mime

    # def mimeTypes(self):
        # return ["application/well-name",]

    def supportedDropActions(self):
        return Qt.CopyAction | Qt.MoveAction

    def setData(self, index: QModelIndex, value, role=None):
        return False

    def flags(self, index: QModelIndex):
        return index.internalPointer().flags()

    def _getEntryPosition(self, entry):
        return self.entries.index(entry)

    def onClicked(self, index: QModelIndex):
        pass

    def onWellsAdded(self):
        self.beginResetModel()
        self._loadWells()
        self.endResetModel()
