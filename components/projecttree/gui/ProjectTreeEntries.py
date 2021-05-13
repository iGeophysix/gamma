import os
import sys

from PySide2.QtCore import Qt
from PySide2.QtGui import QColor, QIcon, QFont
from PySide2.QtWidgets import QMenu, QFileDialog

from datetime import datetime

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.mainwindow.gui import GeoMainWindow
from components.projecttree.gui.ProjectTreeEntry import ProjectTreeEntry, ProjectEntryEnum
from components.importexport.las.las_export import create_las_file

import logging

gamma_logger = logging.getLogger('gamma_logger')


# clas Groups all the wells in the database.
# Currently not used

# class WellManagerEntry(ProjectTreeEntry):
    # def __init__(self, model):
        # """
        # :param QAbstractItemModel model:
            # Used to trigger full model update when adding new wells
        # """

        # ProjectTreeEntry.__init__(self, model)

        # self.project = Project()

        # self._loadWells()


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



class WellEntry(ProjectTreeEntry):
    def __init__(self, model, parent, well_name : str):
        ProjectTreeEntry.__init__(self, model, parent)

        self._well_name = well_name
        self._well = Well(well_name)

        self._loadDatasets()

    def _loadDatasets(self):
        for dataset_name in self._well.datasets:
            self.entries.append(WellDatasetEntry(model=self._model,
                                                 parent=self,
                                                 well=self._well,
                                                 dataset_name=dataset_name))

        self.entries.sort(key=lambda e : e.data(Qt.DisplayRole))


    def data(self, role=Qt.DisplayRole, column=ProjectEntryEnum.NAME.value):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)

        return None


    def flags(self):
        return ProjectTreeEntry.flags(self) | Qt.ItemIsDragEnabled

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._well_name

        # TODO: settings for constructing well name

        return None

    def _getDecorationRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return QIcon.fromTheme('pda')

        return None


# class WellPropertyManagerEntry(ProjectTreeEntry):
    # def __init__(self, model, parent, well : Well):
        # ProjectTreeEntry.__init__(self, model, parent)

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


# class WellPropertyEntry(ProjectTreeEntry):
    # def __init__(self, model, parent, prop : WellProperty):
        # ProjectTreeEntry.__init__(self, model, parent)

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

# class WellDatasetManagerEntry(ProjectTreeEntry):
    # def __init__(self, model, parent, well_name : str, well_info):
        # ProjectTreeEntry.__init__(self, model, parent)

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


class WellDatasetEntry(ProjectTreeEntry):
    def __init__(self, model, parent, well, dataset_name):
        ProjectTreeEntry.__init__(self, model, parent)

        self._well = well
        self._dataset = WellDataset(self._well, dataset_name)

        self._loadCurves()


    def _loadCurves(self):
        for curve_name in self._dataset.log_list:
            self.entries.append(CurveEntry(model=self._model,
                                           parent=self,
                                           dataset=self._dataset,
                                           curve_name=curve_name))

        self.entries.sort(key=lambda e : e.data(Qt.DisplayRole))

    def data(self, role=Qt.DisplayRole, column=ProjectEntryEnum.NAME.value):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)

        return None

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._dataset.name

        return None

    def _getDecorationRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return QIcon.fromTheme('appointment-new')

        return None

    def contextMenu(self):

        menu = QMenu()

        action = menu.addAction("Export to LAS...")
        action.triggered.connect(self.on_export)

        return menu

    def on_export(self):
        root_directory = os.path.dirname(sys.modules['__main__'].__file__)
        fileNameSuggestion = os.path.join(root_directory, self._dataset.name)

        if not fileNameSuggestion.endswith(".las"):
            fileNameSuggestion+=".las"

        file, _ = QFileDialog.getSaveFileName(GeoMainWindow(),
                                              'Select fine name to export',
                                              fileNameSuggestion,
                                              'LAS Files (*.las)')

        curves = []
        for curve_name in self._dataset.log_list:
            curves.append((self._dataset.name, curve_name))

        las = create_las_file(self._well.name, curves)
        las.write(file, version=2)


class CurveEntry(ProjectTreeEntry):
    def __init__(self, model, parent, dataset, curve_name: str):
        ProjectTreeEntry.__init__(self, model, parent)

        self._dataset = dataset
        self._curve_name = curve_name
        self._basic_log = BasicLog(dataset.id, curve_name)

        self._loadMeta()

    def _loadMeta(self):
        meta = self._basic_log.meta

        for m in meta.asdict().keys():
            self.entries.append(MetaEntry(model=self._model,
                                          parent=self,
                                          meta=meta,
                                          meta_key=m))

    def data(self, role=Qt.DisplayRole, column=ProjectEntryEnum.NAME.value):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)
        elif role == Qt.ToolTipRole:
            return self._getToolTipRole(column)

        return None

    def flags(self):
        return ProjectTreeEntry.flags(self) | Qt.ItemIsDragEnabled

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._basic_log.name

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


class MetaEntry(ProjectTreeEntry):
    def __init__(self, model, parent, meta, meta_key):
        ProjectTreeEntry.__init__(self, model, parent)

        self._meta = meta
        self._meta_key = meta_key


    def data(self, role=Qt.DisplayRole, column=ProjectEntryEnum.NAME.value):
        if role == Qt.DisplayRole:
            return self._getDisplayRole(column)
        elif role == Qt.DecorationRole:
            return self._getDecorationRole(column)
        elif role == Qt.ToolTipRole:
            return self._getToolTipRole(column)

        return None

    def _getDisplayRole(self, column):
        if column == ProjectEntryEnum.NAME.value:
            return self._meta_key
        elif column == ProjectEntryEnum.VALUE.value:
            return str(self._meta[self._meta_key])

        return None

    def _getDecorationRole(self, column):
        # if column == ProjectEntryEnum.NAME.value:
            # return QIcon.fromTheme('appointment-new')

        return None

    def _getToolTipRole(self, column):
        return None
