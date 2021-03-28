from enum import Enum

from abc import ABC, abstractmethod

from PySide2.QtCore import Qt


class NoModelException(Exception):
    pass


class ProjectEntryEnum(Enum):
    NAME  = 0
    VALUE = 1


class TreeEntry(ABC):
    def __init__(self, model, parent = None):
        self._model = model
        self._parent = parent
        self._createEntries()

    def parent(self):
        return self._parent

    def positionOfEntry(self, entry):
        n = self.entries.index(entry)

    @abstractmethod
    def data(self, role, column):
        return None

    def flags(self):
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    def _createEntries(self):
        self.entries = []

    def _model(self):
        if (self._parent == None):
            if hasattr(self, '_model'):
                return self._model
            else:
                raise NoModelException()
        else:
            return self._parent._model()




