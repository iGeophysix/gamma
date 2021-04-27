import logging

from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath
from PySide2.QtCore import QPointF, QRectF, Qt

from components.tablet.gui.TabletObject import TabletObject, TabletGraphicsObject
from components.tablet.gui.WellObject import WellObject

from components.domain.Well import Well# , WellProperty

gamma_logger = logging.getLogger("gamma_logger")

# from components.tablet.RegularGridObject import RegularGridObject
# from components.tablet.LogGridObject import LogGridObject
# from components.tablet.GridObject import GridObject
# from components.tablet.DepthColumnObject import DepthColumnObject


class WellGroupObject(TabletObject):

    def __init__(self):
        TabletObject.__init__(self)

        self._head = WellGroupGraphicsObjectHead(self)
        self._body = WellGroupGraphicsObjectBody(self)

        # self.loadWells(wells)


    def setPosition(self):
        TabletObject.setPosition(self)

        br = self._head.scene().itemsBoundingRect()
        self._head.scene().setSceneRect(br)

        br = self._body.scene().itemsBoundingRect()
        self._body.scene().setSceneRect(br)


    def loadWells(self, data):
        """
        :param data: is a netsted dict {well: {dataset: [curve]}}
        """

        existingWells = \
            {wellObject._dbWell.name : wellObject for wellObject in self.children}

        for w, datasetsWithCurves in data.items():

            if not w in existingWells:
                wellObject = WellObject(w, self)
                existingWells[w] = wellObject


            existingWells[w].loadDatasetAndCurves(datasetsWithCurves)

            gamma_logger.info('Loading well "{}"'.format(w))

        self._head.update()
        self._body.update()


    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body

    def boundingRect(self) -> QRectF:

        top, bottom = self.depthRange()
        w = 0
        for c in self.childObjects():
            r = c.boundingRect() 
            w += r.width()

        # TODO spacing object beween wells
        w += max(0, len(self.children) - 1) * 0.01

        answ = QRectF(0, top, w, bottom - top)

        return answ


class WellGroupGraphicsObjectBody(TabletGraphicsObject):

    def __init__(self, well_group):
        TabletGraphicsObject.__init__(self)

        self._well_group = well_group

    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):
        p = QPen(Qt.gray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)
        painter.setBrush(Qt.lightGray)
        # painter.setBrush(Qt.green)

        painter.drawRect(self.boundingRect())


    def boundingRect(self) -> QRectF:
        r = self._well_group.boundingRect()

        if r.isEmpty():
            r = QRectF(0, 0, 0.05, 100)

        return r


class WellGroupGraphicsObjectHead(TabletGraphicsObject):

    def __init__(self, well_group):
        TabletGraphicsObject.__init__(self)

        self._well_group = well_group

    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):
        p = QPen(Qt.gray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)
        painter.setBrush(Qt.lightGray)
        # painter.setBrush(Qt.green)

        painter.drawRect(self.boundingRect())

    def boundingRect(self) -> QRectF:
        """ width_of_all_grids X max_depth_in_meters """

        r = self._well_group.boundingRect()

        h = 0
        for c in self.childItems():
            rr = c.boundingRect()
            h = max(h, rr.height())

        r = QRectF(0, 0, r.width(), h)

        if r.isEmpty():
            r = QRectF(0, 0, 0.05, 0.01)

        return r
