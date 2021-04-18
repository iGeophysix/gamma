from PySide2.QtCore import Qt
from PySide2.QtGui import QColor, QIcon, QFont

from datetime import datetime

from components.projecttree.gui.ProjectTreeEntry import TreeEntry, ProjectEntryEnum
from components.database.gui.DbEventDispatcherSingleton import DbEventDispatcherSingleton
from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.domain.Well import Well


import logging

gamma_logger = logging.getLogger('gamma_logger')


# clas Groups all the wells in the database.
# Currently not used

# class WellManagerEntry(TreeEntry):
    # def __init__(self, model):
        # """
        # :param QAbstractItemModel model:
            # Used to trigger full model update when adding new wells
        # """

        # TreeEntry.__init__(self, model)

        # self.project = Project("Default Project")

        # self._loadWells()

        # DbEventDispatcherSingleton().wellsAdded.connect(self.onWellsAdded)

    # def _loadWells(self):
        # self.entries = []
        # wells = self.project.list_wells()
        # for well_name, well_info in wells.items():
            # self.entries.append(WellEntry(model = self._model,
                                          # parent = self,
                                          # well_name=well_name,
                                          # well_info=well_info))
        # print(self.entries)

    # def onWellsAdded(self):
        # self._model.beginResetModel()
        # self._loadWells()
        # self._model.endResetModel()

    # def data(self, role, column):
        # if role == Qt.DisplayRole:
            # return self._getDisplayRole(column)
        # elif role == Qt.DecorationRole:
            # return self._getDecorationRole(column)

        # return None

    # def _getDisplayRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # return 'Well Manager'

        # return None

    # def _getDecorationRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # return QIcon.fromTheme('drive-harddisk')

        # return None



class WellEntry(TreeEntry):
    def __init__(self, model, parent, well_name : str, well_info):
        TreeEntry.__init__(self, model, parent)

        self._well_name = well_name
        self._well_info = well_info

        _s = RedisStorage()
        for ds_id in self._well_info["datasets"]:
            dataset_info = _s.get_dataset_info(dataset_id=ds_id)
            dataset_name = dataset_info['name']
            dataset_logs = _s.get_logs_meta(ds_id)

            self.entries.append(WellDatasetEntry(model = self._model,
                                                 parent = self,
                                                 dataset_info=dataset_info,
                                                 dataset_name=dataset_name,
                                                 dataset_logs=dataset_logs))


    def data(self, role, column):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)

        return None


    def flags(self):
        return TreeEntry.flags(self) | Qt.ItemIsDragEnabled

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._well_name

        # TODO: settings for constructing well name

        return None

    def _getDecorationRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return QIcon.fromTheme('pda')

        return None


# class WellPropertyManagerEntry(TreeEntry):
    # def __init__(self, model, parent, well : Well):
        # TreeEntry.__init__(self, model, parent)

        # self._well = well

        # self._loadWellProperties()

    # def _loadWellProperties(self):
        # for p in self._well.well_properties:
            # self.entries.append(WellPropertyEntry(model = self._model,
                                                  # parent = self,
                                                  # prop = p))

    # def data(self, role, column):
        # if role == Qt.DisplayRole:
            # if column == ProjectEntryEnum.NAME.value:
                # return 'Properties'
        # elif role == Qt.DecorationRole:
            # if column == ProjectEntryEnum.NAME.value:
                    # return QIcon.fromTheme('accessories-text-editor')

        # return None


# class WellPropertyEntry(TreeEntry):
    # def __init__(self, model, parent, prop : WellProperty):
        # TreeEntry.__init__(self, model, parent)

        # self._prop = prop

    # def data(self, role, column):
        # if role == Qt.DisplayRole:
            # return self._getDisplayRole(column)
        # elif role == Qt.FontRole:
            # return self._getFontRole(column)

        # return None

    # def _getDisplayRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # return self._prop.name
        # elif column == ProjectEntryEnum.VALUE.value:
            # if self._prop.unit:
                # return '{} [{}]'.format(self._prop.value,
                                        # self._prop.unit.symbol)
            # else:
                # return self._prop.value

        # return None

    # def _getFontRole(self, column):

        # if column == ProjectEntryEnum.NAME.value:
            # if self._prop.well_property_type:
                # if self._prop.well_property_type.key_property:
                    # f = QFont()
                    # f.setBold(True)
                    # return f

        # return None

# Class groups all the datasets.
# Not used at the time.

# class WellDatasetManagerEntry(TreeEntry):
    # def __init__(self, model, parent, well_name : str, well_info):
        # TreeEntry.__init__(self, model, parent)

        # self._well_name = well_name
        # self._well_info = well_info

        # self._loadWellDatasets()

    # def _loadWellDatasets(self):
        # _s = RedisStorage()
        # for ds_id in self._well_info["datasets"]:
            # dataset_info = _s.get_dataset_info(dataset_id=ds_id)
            # dataset_name = dataset_info['name']
            # dataset_logs = _s.get_logs_meta(ds_id)

            # self.entries.append(WellDatasetEntry(model = self._model,
                                                 # parent = self,
                                                 # dataset_info=dataset_info,
                                                 # dataset_name=dataset_name,
                                                 # dataset_logs=dataset_logs))

    # def data(self, role, column):
        # if role == Qt.DisplayRole:
            # if column == ProjectEntryEnum.NAME.value:
                # return 'WellDatasets'
        # elif role == Qt.DecorationRole:
            # if column == ProjectEntryEnum.NAME.value:
                    # return QIcon.fromTheme('media-record')

        # return None


class WellDatasetEntry(TreeEntry):
    def __init__(self, model, parent, dataset_info, dataset_name, dataset_logs):
        TreeEntry.__init__(self, model, parent)

        self._dataset_info = dataset_info
        self._dataset_name = dataset_name
        self._dataset_logs = dataset_logs

        self._loadCurves()


    def _loadCurves(self):
        for curve in self._dataset_logs.keys():
            self.entries.append(CurveEntry(model = self._model,
                                           parent = self,
                                           curve = curve))

    def data(self, role, column):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)

        return None

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._dataset_name

        return None

    def _getDecorationRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return QIcon.fromTheme('appointment-new')

        return None


class CurveEntry(TreeEntry):
    def __init__(self, model, parent, curve : str):
        TreeEntry.__init__(self, model, parent)

        self._curve = curve

    def data(self, role, column):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)
        elif role == Qt.ToolTipRole:
            return self._getToolTipRole(column)

        return None

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._curve

        return None

    def _getDecorationRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # return QIcon.fromTheme('appointment-new')

        return None

    def _getToolTipRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # ff = "<div><img width='300' height='400' src='data:image/png;base64,{}'></div>"
            # ff = ff.format(base64.b64encode(self._curve.preview).decode('ascii'))
            # return ff

        return None


