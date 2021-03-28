from abc import ABC, abstractmethod

from PySide2.QtCore import QRectF

from components.tablet.gui.TabletObject import TabletObject 

from typing import Tuple


class GridObject(TabletObject):

    def __init__(self, parent = None):
        TabletObject.__init__(self, parent)

    @abstractmethod
    def width(self):
        pass

    def boundingRect(self) -> QRectF:
        """ width_in_curve_units X depth_in_meters """

        h0, h1 = self.depthRange()

        answ = QRectF(0, h0, self.width(), h1 - h0)

        return answ
