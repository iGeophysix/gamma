from PySide2.QtCore import Qt, QRectF, QPoint, Signal, Slot, QEvent

from PySide2.QtWidgets import QWidget, QGraphicsView, QOpenGLWidget, QScrollBar

from PySide2.QtGui import (
        QDragEnterEvent,
        QDragMoveEvent,
        QDropEvent,
        QMouseEvent,
        QPainter,
        QSurfaceFormat,
        QTransform,
        QWheelEvent,
        )


# from PySide2.QtOpenGL import QGL, QGLFormat

import math


class TabletView(QGraphicsView):

    def __init__(self, parent : QWidget = None):
        QGraphicsView.__init__(self, parent)

        # glWidget = QOpenGLWidget()
        # f = QSurfaceFormat()
        # f.setSamples(4)
        # glWidget.setFormat(f)
        # self.setViewport(glWidget)

        # self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setRenderHint(QPainter.Antialiasing)

        self.setBackgroundBrush(Qt.lightGray)

        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)

        self.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)

        # self.setOptimizationFlags(QGraphicsView.DontSavePainterState)
        self.setViewportUpdateMode(QGraphicsView.FullViewportUpdate)
        # self.setViewportUpdateMode(QGraphicsView.SmartViewportUpdate)

        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)

        # self.setCacheMode(QGraphicsView.CacheBackground)


    def dotsPerMeter(self):
        return self.physicalDpiX() / 0.0254

    def drawBackground(self, painter : QPainter, r : QRectF):
        QGraphicsView.drawBackground(self, painter, r)



class TabletViewHead(TabletView):
    def __init__(self, parent : QWidget = None):
        TabletView.__init__(self, parent)

        self.scale(self.dotsPerMeter(), self.dotsPerMeter())

    @Slot(QTransform)
    def onTransformed(self, t):
        """
        Sync tranformations with the body.
        """
        myTransform = self.transform()

        myTransform.translate(-myTransform.m13() + t.m12(), 0.0)
        myTransform.scale(t.m11() / myTransform.m11(), 1.0)

        self.setTransform(myTransform)



class TabletViewBody(TabletView):

    transformed = Signal(QTransform)

    def __init__(self, parent : QWidget = None):
        TabletView.__init__(self, parent)

        self.scale(self.dotsPerMeter(), self.dotsPerMeter() / 500.0)
        # print("table transform", self.transform())

        self._scale_multiplier = 1.2

        self.viewport().setAcceptDrops(True)

        self.viewport().installEventFilter(self)


    def wheelEvent(self, event : QWheelEvent):

        delta = event.angleDelta();
        if delta.y() == 0:
            event.ignore()
            return
        d = delta.y() / math.fabs(delta.y())

        if event.modifiers() & Qt.ControlModifier or \
           event.modifiers() & Qt.ShiftModifier:

            if d > 0.0:
                self._scaleUp(event);
            else:
                self._scaleDown(event);

        else:
            QGraphicsView.wheelEvent(self, event)

    def eventFilter(self, obj, event : QEvent):
        if event.type() == QEvent.MouseButtonPress:
            # Enter here any button you like
            if event.button() == Qt.MiddleButton:
                # temporarly enable dragging mode
                self.setDragMode(QGraphicsView.ScrollHandDrag)
                # emit a left mouse click (the default button for the drag mode)
                pressEvent = QMouseEvent(QEvent.GraphicsSceneMousePress,
                                         event.pos(),
                                         Qt.LeftButton,
                                         Qt.LeftButton,
                                         Qt.NoModifier)

                self.mousePressEvent(pressEvent)
        elif event.type() == QEvent.MouseButtonRelease:
            # # disable drag mode if dragging is finished
            self.setDragMode(QGraphicsView.NoDrag)

            return False
        else:
            return QGraphicsView.eventFilter(self, obj, event)


    def _scaleUp(self, event):

        x_factor = self._scale_multiplier

        if event.modifiers() & Qt.ShiftModifier:
            x_factor = 1.0

        self.scale(x_factor, self._scale_multiplier)

        t = self.transform()

        self.transformed.emit(t)

    def _scaleDown(self, event):

        x_factor = 1.0 / self._scale_multiplier

        if event.modifiers() & Qt.ShiftModifier:
            x_factor = 1.0

        self.scale(x_factor, 1.0 / self._scale_multiplier)

        t = self.transform()
        self.transformed.emit(t)
