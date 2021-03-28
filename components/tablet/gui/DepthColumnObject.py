from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath, QFont
from PySide2.QtCore import QPointF, QRectF, Qt, QLineF

from components.tablet.gui.TabletObject import TabletObject, TabletGraphicsObject
from components.tablet.gui.CurveObject import CurveObject
from components.tablet.gui.GridObject import GridObject


class DepthColumnObject(TabletObject):

    def __init__(self, parent):
        TabletObject.__init__(self, parent)

        self._head = DepthColumnGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = DepthColumnGraphicsObjectBody(parent.bodyGraphicsObject(), self)


    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body

    def width(self):
        return 0.02 # 2 cm


    def boundingRect(self) -> QRectF:
        """ fixed_width X max_depth_in_meters """

        depthRange = self.fullSiblingDepthRange()

        pbr = QRectF(QPointF(0.0, depthRange[0]),
                     QPointF(self.width(), depthRange[1]))
        return pbr


class DepthColumnGraphicsObjectHead(TabletGraphicsObject):

    def __init__(self, parent, depthCol):
        TabletGraphicsObject.__init__(self, parent)

        self._depthCol = depthCol

        self.setFlag(QGraphicsItem.ItemUsesExtendedStyleOption)

    def boundingRect(self) -> QRectF:
        br = self._depthCol.boundingRect()

        h = 0.0
        for g in self._depthCol.parent().childObjects():
            if isinstance(g, GridObject):
                h = max(h, g.headGraphicsObject().boundingRect().height())

        br.setTop(0.0)
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


class DepthColumnGraphicsObjectBody(TabletGraphicsObject):

    def __init__(self, parent, depthCol):
        TabletGraphicsObject.__init__(self, parent)

        self._depthCol = depthCol

        self.setFlag(QGraphicsItem.ItemUsesExtendedStyleOption)

    def boundingRect(self) -> QRectF:
        return self._depthCol.boundingRect()

    def adjustExposedRectForHorizontalLines(self, exposedRect, step):
        depthRange = self._depthCol.fullSiblingDepthRange()

        br = QRectF(QPointF(0.0, depthRange[0]),
                    QPointF(self._depthCol.width(), depthRange[1]))

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
            line = QLineF(exposedRect.left(), y,
                          exposedRect.right(), y)

            painter.drawLine(line)

            y += step.y()

        painter.save()

        t = painter.transform()

        font = QFont("Monospace")
        font.setStyleHint(QFont.TypeWriter)
        scale = t.m11() / self.dotsPerMeter()
        font.setPointSizeF(14.0 * min(scale, 1.5)) 

        painter.setFont(font)

        y = exposedRect.top()
        while y < exposedRect.bottom():
            painter.save()
            painter.resetTransform()
            p = t.map(QPointF(0.0, y))
            painter.translate(p)
            painter.drawText(2, -2, "{0:.2f}".format(y))
            painter.restore()
            y += step.y()
        painter.restore()

    def paint(self,
              painter : QPainter,
              option : QStyleOptionGraphicsItem,
              widget : QWidget):

        dpm = self.dotsPerMeter()
        (inv_t, _) = self.view().transform().inverted()

        step = QPointF(0.02 * dpm, 0.02 * dpm)
        step = inv_t.map(step)

        exposedRect = \
            self.adjustExposedRectForHorizontalLines(option.exposedRect, step)

        # p = QPen(col)
        p = QPen(Qt.lightGray)
        p.setCosmetic(True)
        p.setWidthF(1.0)
        painter.setPen(p)

        self.horizontalLoop(painter, exposedRect, step)

        p.setColor(Qt.gray)
        painter.drawRect(self.boundingRect())
