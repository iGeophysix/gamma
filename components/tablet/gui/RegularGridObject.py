import random

from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        QGraphicsItem,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath, QTransform, QColor
from PySide2.QtCore import QPointF, QRectF, QLineF, Qt

from components.tablet.gui.GridObject import GridObject
from components.tablet.gui.TabletObject import TabletGraphicsObject


class RegularGridObject(GridObject):

    def __init__(self, parent):
        GridObject.__init__(self, parent)

        self._head = RegularGridGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = RegularGridGraphicsObjectBody(parent.bodyGraphicsObject(), self)


        # self._setPosition()
        self.getRoot().setPosition()

    def setPosition(self):

        GridObject.setPosition(self)

        w = 0
        for it in self.parent().childObjects()[:self.indexByParent()]:
            w += it.boundingRect().width()

        self._head.setPos(w, 0)
        self._body.setPos(w, 0)


    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body


    def width(self):
        return 0.05 # 5 cm


class RegularGridGraphicsObjectBody(TabletGraphicsObject):

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

        self.drawVerticalLines(painter, option.exposedRect)
        self.drawHorizontalLines(painter, option.exposedRect)


    def adjustExposedRectForVerticalLines(self, exposedRect, step):
        depthRange = self._grid.fullSiblingDepthRange()
        br = QRectF(QPointF(0.0, depthRange[0]),
                    QPointF(self._grid.width(), depthRange[1]))

        exposedTop = max(br.top(), exposedRect.top())
        exposedBottom = min(br.bottom(), exposedRect.bottom())

        exposedLeft = max(br.left(), exposedRect.left())
        exposedLeft = (exposedLeft // step.x() + 1) * step.x()
        exposedRight = min(br.right(), exposedRect.right())
        exposedRight = (exposedRight // step.x() + 1) * step.x()

        return QRectF(QPointF(exposedLeft, exposedTop),
                      QPointF(exposedRight, exposedBottom))


    def verticalLoop(self, painter, exposedRect, step):
        x = exposedRect.left()
        while x < exposedRect.right():
            l = QLineF(x, exposedRect.top(),
                       x, exposedRect.bottom())

            painter.drawLine(l)

            x += step.x()


    def drawVerticalLines(self, painter : QPainter, exposedRect):

        dpm = self.dotsPerMeter()
        (inv_t, _) = self.view().transform().inverted()

        step = QPointF(0.005 * dpm, 0.005 * dpm)
        step = inv_t.map(step)

        exposedRect = self.adjustExposedRectForVerticalLines(exposedRect, step)

        p = QPen(Qt.lightGray)
        p.setStyle(Qt.DashLine)
        p.setCosmetic(True)
        p.setWidthF(0.5)
        painter.setPen(p)

        self.verticalLoop(painter, exposedRect, step)

        ###

        step = QPointF(0.02 * dpm, 0.02 * dpm)
        step = inv_t.map(step)

        exposedRect = self.adjustExposedRectForVerticalLines(exposedRect, step)

        p = QPen(Qt.lightGray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)

        self.verticalLoop(painter, exposedRect, step)


    def adjustExposedRectForHorizontalLines(self, exposedRect, step):
        # depthRange = self._grid.parent().depthRange()
        depthRange = self._grid.fullSiblingDepthRange()
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



class RegularGridGraphicsObjectHead(TabletGraphicsObject):

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
