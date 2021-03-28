from PySide2.QtWidgets import (
        QGraphicsItem,
        QStyleOptionGraphicsItem,
        QWidget,
        )
from PySide2.QtGui import QPainter, QPen, QPainterPath, QTransform

from PySide2.QtCore import QPointF, QRectF, Qt

from abc import ABC, abstractmethod

from typing import Tuple


class TabletObject(ABC):

    def __init__(self, parent = None):
        self.children = []
        self.setParentTabletObject(parent)

    def parent(self):
        return self._parent


    @abstractmethod
    def headGraphicsObject(self):
        pass

    @abstractmethod
    def bodyGraphicsObject(self):
        pass

    def depthRange(self) -> Tuple[float, float]:
        '''
        Returns (top, bottom) range of depths.
        '''
        h0 = float("inf") # is combined later with min()
        h1 = float("-inf") # is combined later with max()


        for child in self.childObjects():
            top, bottom = child.depthRange()

            h0 = min(h0, top)
            h1 = max(h1, bottom)

        return (h0, h1)

    def fullSiblingDepthRange(self) -> Tuple[float, float]:
        '''
        Visits all the siblings and expands common depth range
        '''
        (h0, h1) = self.depthRange()

        for sibling in self.parent().childObjects():
            (hs0, hs1) = sibling.depthRange()
            h0 = min(h0, hs0)
            h1 = max(h1, hs1)

        return (h0, h1)





    def setParentTabletObject(self, parent):

        self._parent = parent

        if parent:
            parent.addChild(self)

    def addChild(self, child):
        self.children.append(child)

    def childObjects(self):
        return self.children



class TabletGraphicsObject(QGraphicsItem):

    def __init__(self, parent = None):
        QGraphicsItem.__init__(self)

        self.setParentItem(parent)

        # for c in children:
            # addChild(c)

        # self.setCacheMode(QGraphicsItem.DeviceCoordinateCache)

    def view(self):
        return self.scene().views()[0]

    def dotsPerMeter(self):
        return self.view().physicalDpiX() / 0.0254

    def boundingRect(self) -> QRectF:
        return self.childrenBoundingRect()
