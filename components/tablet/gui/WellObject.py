from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath
from PySide2.QtCore import QPointF, QRectF, Qt

from components.tablet.gui.CurveObject import CurveObject
from components.tablet.gui.DepthColumnObject import DepthColumnObject
from components.tablet.gui.GridObject import GridObject
from components.tablet.gui.LogGridObject import LogGridObject
from components.tablet.gui.RegularGridObject import RegularGridObject
from components.tablet.gui.TabletObject import TabletObject, TabletGraphicsObject

from components.domain.Well import Well
from components.domain.WellDataset import WellDataset

from typing import Tuple


class WellObject(TabletObject):

    def __init__(self, dbWell, parent):
        TabletObject.__init__(self, parent)

        self._dbWell = dbWell

        self._head = WellGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = WellGraphicsObjectBody(parent.bodyGraphicsObject(), self)

        dc = DepthColumnObject(self)


        # for dbCurve in (dbWell.curves[0], dbWell.curves[1]):
        datasets = dbWell.datasets

        for ds in datasets:
        # dbCurve = dbWell.curves[0]

            dataset = WellDataset(self._dbWell, ds)

            log_list = dataset.get_log_list()

            for log in log_list:
                grid = RegularGridObject(self)
                curve = CurveObject(grid, self._dbWell, dataset, log)

        # if len(datasets) > 1:
            # dbCurve = dbWell.curves[1]
            # curve = CurveObject(grid, dbCurve)

        # grid = LogGridObject(self)
        # for dbCurve in (dbWell.curves[2], dbWell.curves[3],):
            # curve = CurveObject(grid, dbCurve)

        # grid = RegularGridObject(self)
        # for dbCurve in (dbWell.curves[0], dbWell.curves[3],):
            # curve = CurveObject(grid, dbCurve)

        w = 0
        for it in parent.childObjects()[:-1]:
            w += it.boundingRect().width() + 0.01 ## TODO Better add some spacing object instead of 0.01

        self._head.moveBy(w, 0)
        self._body.moveBy(w, 0)

    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body

    def boundingRect(self) -> QRectF:
        """ width_of_all_grids X max_depth_in_meters """

        top, bottom = self.depthRange()
        w = 0
        for c in self.childObjects():
            r = c.boundingRect()
            w += r.width()

        answ = QRectF(0, top, w, bottom - top)

        return answ


class WellGraphicsObjectBody(TabletGraphicsObject):

    def __init__(self, parent, wellObject):
        TabletGraphicsObject.__init__(self, parent)

        self._wellObject = wellObject


    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):
        p = QPen(Qt.black)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)
        painter.setBrush(Qt.white)

        painter.drawRect(self.boundingRect())


    def boundingRect(self) -> QRectF:
        return self._wellObject.boundingRect()


class WellGraphicsObjectHead(TabletGraphicsObject):

    def __init__(self, parent, wellObject):
        TabletGraphicsObject.__init__(self, parent)

        self._wellObject = wellObject

    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):
        p = QPen(Qt.black)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)
        painter.setBrush(Qt.white)

        painter.drawRect(self.boundingRect())

    def boundingRect(self) -> QRectF:
        r = self._wellObject.boundingRect()
        """ width_of_all_grids X max_depth_in_meters """

        w = 0
        h = 0
        for c in self.childItems():
            r = c.boundingRect()
            w += r.width()
            h = max(h, r.height())

        return QRectF(0, 0, r.width(), r.height())
