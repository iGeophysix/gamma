import random
import numpy as np

from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        QGraphicsItem,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath, QTransform, QColor
from PySide2.QtCore import QPointF, QRectF, QLineF, Qt

from components.tablet.gui.TabletObject import TabletObject, TabletGraphicsObject
from components.tablet.gui.GridObject import GridObject



class LogGridObject(GridObject):

    def __init__(self, parent):
        GridObject.__init__(self, parent)

        self._head = LogGridGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = LogGridGraphicsObjectBody(parent.bodyGraphicsObject(), self)

        w = 0
        for it in parent.childObjects()[:-1]:
            w += it.boundingRect().width()
        self._head.moveBy(w, 0)
        self._body.moveBy(w, 0)


    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body

    def width(self):
        return 0.05 # 5 cm

    def curvesLog10Min(self):

        left = float("inf")

        for c in self.childObjects():
            left = min(left, c.arrayRect().left())

        return left



class LogGridGraphicsObjectBody(TabletGraphicsObject):

    def __init__(self, parent, grid):
        TabletGraphicsObject.__init__(self, parent)

        self._grid = grid

        # self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QGraphicsItem.ItemUsesExtendedStyleOption)


    def boundingRect(self) -> QRectF:
        return self._grid.boundingRect()


    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):

        """
        exposedRect is depth_range (meters) x columnt_print_width (meters)
        """

        self.drawVerticalLines(painter, option.exposedRect)
        self.drawHorizontalLines(painter, option.exposedRect)


    def adjustExposedRectForVerticalLines(self, exposedRect):
        depthRange = self._grid.parent().depthRange()
        br = QRectF(QPointF(0.0, depthRange[0]),
                    QPointF(self._grid.width(), depthRange[1]))

        exposedTop = max(br.top(), exposedRect.top())
        exposedBottom = min(br.bottom(), exposedRect.bottom())

        exposedLeft = max(br.left(), exposedRect.left())
        exposedRight = min(br.right(), exposedRect.right())

        return QRectF(QPointF(exposedLeft, exposedTop),
                      QPointF(exposedRight, exposedBottom))

    def verticalLoop(self, painter, exposedRect, step):
        curveToGrid = self._grid.childObjects()[0]._arrayTransform
        (gridToCurve, _) = curveToGrid.inverted()

        # curveMin = self._grid.curvesLog10Min()

        exposedRectCurve = gridToCurve.mapRect(exposedRect)

        curveMinLog = np.floor(exposedRectCurve.left())
        offset = 10.0 + (np.floor(exposedRectCurve.left() * 10.0))

        if offset < step:
            offset = 1.0
            curveMinLog += 1.0

        # print("Curve min log", curveMinLog)
        # print("offset", offset)

        toGrid = lambda x : x * curveToGrid.m11() + curveToGrid.m31()

        x = toGrid(curveMinLog)

        # grid left to curve left
        while x < exposedRect.right():

            x = toGrid(curveMinLog + np.log10(offset))

            l = QLineF(x, exposedRect.top(),
                       x, exposedRect.bottom())

            painter.drawLine(l)

            offset += step

            if offset >= 10.0:
                offset = 1.0
                curveMinLog += 1.0


    def drawVerticalLines(self, painter : QPainter, exposedRect):

        dpm = self.dotsPerMeter()
        (inv_t, _) = self.view().transform().inverted()

        exposedRect = self.adjustExposedRectForVerticalLines(exposedRect)

        p = QPen(Qt.lightGray)
        p.setStyle(Qt.DashLine)
        p.setCosmetic(True)
        p.setWidthF(0.5)
        painter.setPen(p)

        self.verticalLoop(painter, exposedRect, 1.0)

        ###

        exposedRect = self.adjustExposedRectForVerticalLines(exposedRect)

        p = QPen(Qt.lightGray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)

        self.verticalLoop(painter, exposedRect, 10.0)



    def adjustExposedRectForHorizontalLines(self, exposedRect, step):
        depthRange = self._grid.parent().depthRange()
        br = QRectF(QPointF(0.0, depthRange[0]),
                    QPointF(self._grid.width(), depthRange[1]))

        exposedTop = max(br.top(), exposedRect.top())
        exposedTop = (exposedTop // step.y() + 1) * step.y()
        exposedBottom = min(br.bottom(), exposedRect.bottom())
        exposedBottom = (exposedBottom // step.y() + 1) * step.y()

        exposedLeft = max(br.left(), exposedRect.left())
        exposedRight = min(br.right(), exposedRect.right())

        return QRectF(QPointF(exposedLeft, exposedTop),
                      QPointF(exposedRight, exposedBottom))


    def horizontalLoop(self, painter, exposedRect, step):
        y = exposedRect.top()
        while y < exposedRect.bottom():
            l = QLineF(exposedRect.left(), y,
                       exposedRect.right(), y)

            painter.drawLine(l)

            y += step.y()


    def drawHorizontalLines(self, painter : QPainter, exposedRect):

        dpm = self.dotsPerMeter()
        (inv_t, _) = self.view().transform().inverted()

        step = QPointF(0.005 * dpm, 0.005 * dpm)
        step = inv_t.map(step)

        exposedRect = self.adjustExposedRectForHorizontalLines(exposedRect, step)

        # print("H", exposedRect)

        p = QPen(Qt.lightGray)
        p.setStyle(Qt.DashLine)
        p.setCosmetic(True)
        p.setWidthF(0.5)
        painter.setPen(p)

        self.horizontalLoop(painter, exposedRect, step)

        ###

        step = QPointF(0.02 * dpm, 0.02 * dpm)
        step = inv_t.map(step)

        exposedRect = self.adjustExposedRectForHorizontalLines(exposedRect, step)

        p = QPen(Qt.lightGray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)

        self.horizontalLoop(painter, exposedRect, step)


class LogGridGraphicsObjectHead(TabletGraphicsObject):

    def __init__(self, parent, grid):
        TabletGraphicsObject.__init__(self, parent)

        self._grid = grid

        # self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)
        self.setFlag(QGraphicsItem.ItemUsesExtendedStyleOption)


    def boundingRect(self) -> QRectF:
        br = self._grid.boundingRect()

        br.setTop(0.0)
        br.setLeft(0.0)

        height_per_curve = 0.01 # 1 cm
        h = height_per_curve * len(self._grid.childObjects())

        br.setHeight(h)

        return br


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
