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

from typing import Tuple


class WellObject(TabletObject):

    def __init__(self, wellName, parent):
        TabletObject.__init__(self, parent)

        self._dbWell = Well(wellName)

        self._head = WellGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = WellGraphicsObjectBody(parent.bodyGraphicsObject(), self)

        dc = DepthColumnObject(self)

        self.getRoot().setPosition()


    def loadDatasetAndCurves(self, datasetsWithCurves):

        curvesWithDatasets = {}
        for d, cs in datasetsWithCurves.items():
            for c in cs:
                curvesWithDatasets[c] = d


        existingCurvesWithDatasets = {}
        for c in self.children:
            if isinstance(c, GridObject):
                for cc in c.children:
                    if isinstance(cc, CurveObject):
                        existingCurvesWithDatasets[cc.curveName] = cc.datasetName

        for c, d in curvesWithDatasets.items():
            if not c in existingCurvesWithDatasets or \
                    existingCurvesWithDatasets[c] != d:

                grid = RegularGridObject(self)
                curve = CurveObject(grid, self._dbWell, d, c)

        # if len(datasets) > 1:
            # dbCurve = dbWell.curves[1]
            # curve = CurveObject(grid, dbCurve)

        # grid = LogGridObject(self)
        # for dbCurve in (dbWell.curves[2], dbWell.curves[3],):
            # curve = CurveObject(grid, dbCurve)

        # grid = RegularGridObject(self)
        # for dbCurve in (dbWell.curves[0], dbWell.curves[3],):
            # curve = CurveObject(grid, dbCurve)


        self.getRoot().setPosition()


    def setPosition(self):

        TabletObject.setPosition(self)

        w = 0
        for it in self.parent().childObjects()[:self.indexByParent()]:
            ## TODO Better add some spacing object instead of 0.01
            w += it.boundingRect().width() + 0.01

        self._head.setPos(w, 0)
        self._body.setPos(w, 0)

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
