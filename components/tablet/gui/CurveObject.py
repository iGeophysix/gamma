import math
import random

import numpy as np
from PySide2.QtCore import QPointF, QRectF, QLineF
from PySide2.QtGui import (
    QColor,
    QFont,
    QPainter,
    QPainterPath,
    QPen,
    QTransform,
)
from PySide2.QtWidgets import (
    QStyleOptionGraphicsItem,
    QWidget,
)

# from components.domain.Curve import Curve
from components.domain.Log import BasicLog
from components.domain.WellDataset import WellDataset
from components.importexport.FamilyProperties import FamilyProperties
from components.tablet.gui.LogGridObject import LogGridObject
from components.tablet.gui.TabletObject import TabletObject, TabletGraphicsObject


class CurveObject(TabletObject):
    """
    Class member _arrayTransform maps curve rect (values X depths)
    onto grid coordinate system (for example, 0 - 5 cm in width)
    """

    def __init__(self, parent, dbWell, datasetName, curveName):
        TabletObject.__init__(self, parent)

        self._dbWell = dbWell
        self._dbDataset = WellDataset(dbWell, datasetName)
        self._curveName = curveName
        self._log = BasicLog(self._dbDataset.id, self._curveName)

        self._arrayRect = None

        self._head = CurveGraphicsObjectHead(parent.headGraphicsObject(), self)
        self._body = CurveGraphicsObjectBody(parent.bodyGraphicsObject(), self)

        self.computeArrayTransform()

    @property
    def datasetName(self):
        return self._dbDataset.name

    @property
    def curveName(self):
        return self._curveName

    def depthRange(self):
        return (self.arrayRect().top(), self.arrayRect().bottom())

    def minMax(self):
        if hasattr(self._log.meta, 'display'):
            min_ = self._log.meta.display.get('min', None) if self._log.meta.display.get('min', None) is not None else np.nanmin(self._log.values[:, 1])
            max_ = self._log.meta.display.get('max', None) if self._log.meta.display.get('max', None) is not None else np.nanmax(self._log.values[:, 1])
            return min_, max_
        elif FamilyProperties.exists(self._log.meta.family):
            fam = FamilyProperties()[self._log.meta.family]
            min_ = fam['min'] if fam.get('min', None) is not None else np.nanmin(self._log.values[:, 1])
            max_ = fam['max'] if fam.get('max', None) is not None else np.nanmax(self._log.values[:, 1])
            return min_, max_
        else:
            return self._log.meta.basic_statistics['min_value'], self._log.meta.basic_statistics['max_value']

    def computeArrayTransform(self):
        ## TODO: TEMPORARY, until I implement min max in DB
        mi, ma = self.minMax()

        scale = self.parent().width() / (ma - mi)

        # TODO: HERE add offset when doesn't not start at parent's min

        t = QTransform.fromScale(scale, 1.0)

        self._arrayTransform = t.translate(-mi, 0.0)

        h = 0
        for it in self.parent().childObjects()[:-1]:
            h += it.headGraphicsObject().boundingRect().height()
        self._head.moveBy(0, h)

    def color(self):
        if hasattr(self._log.meta, 'display'):
            r, g, b = self._log._meta.display.get('color', (0, 0, 0))
            self._color = QColor.fromRgb(r, g, b)
        elif FamilyProperties.exists(self._log.meta.family):
            fam = FamilyProperties()[self._log.meta.family]
            r, g, b = fam.get('color', (0, 0, 0))
            self._color = QColor.fromRgb(r, g, b)
        elif not hasattr(self, "_color"):
            c = lambda: random.randint(0, 255)
            self._color = QColor.fromRgb(c(), c(), c())

        return self._color

    def headGraphicsObject(self):
        return self._head

    def bodyGraphicsObject(self):
        return self._body

    def isLogScale(self) -> bool:
        return isinstance(self.parent(), LogGridObject)

    def array(self):
        """ log10-Facade for curve representation """
        array = self._log.values

        if self.isLogScale():
            a = array[..., 1]
            a[np.less_equal(a, 0.0, where=~np.isnan(a))] = np.nan
            array[..., 1] = np.log10(array[..., 1])

        # clip left and right boundaries
        mi, ma = self.minMax()
        array[:, 1] = np.clip(array[:, 1], mi, ma)

        return array

    def arrayRect(self) -> QRectF:

        if not self._arrayRect:
            array = self.array()
            mi, ma = self.minMax()
            min_ = (mi, np.nanmin(array[:, 0]))
            max_ = (ma, np.nanmax(array[:, 0]))

            if math.isclose(min_[0], max_[0]):
                magnitude = round(math.log(max(1, abs(max_[0])), 10))
                dx = 10. ** magnitude * 0.5
                min_[0] = min_[0] - dx
                max_[0] = max_[0] + dx

            self._arrayRect = QRectF(QPointF(*min_), QPointF(*max_))

        return self._arrayRect

    def boundingRect(self) -> QRectF:
        """ width_in_curve_units X depth_in_meters """
        ar = self.arrayRect()
        ar = self._arrayTransform.mapRect(ar)
        return ar


class CurveGraphicsObjectBody(TabletGraphicsObject):

    def __init__(self, parent, curve_object):
        TabletGraphicsObject.__init__(self, parent)

        self._curve_object = curve_object

        self._constructPathAndPoints()

    def _constructPathAndPoints(self):
        self._path = None
        self.points = []

        previous_nan = True

        array = self._curve_object.array()

        for p in array:
            if not np.isnan(p).any():
                p = np.flip(p)

                p = QPointF(*p)
                if not self._path:
                    self._path = QPainterPath(p)

                if previous_nan:
                    self._path.moveTo(p)
                else:
                    self._path.lineTo(p)

                self.points.append(p)

                previous_nan = False

            else:
                previous_nan = True

        self._subpathPolygones = self._path.toSubpathPolygons()

    def boundingRect(self) -> QRectF:  # TODO: Not used?
        return self._curve_object.boundingRect()

    def paint(self,
              painter: QPainter,
              option: QStyleOptionGraphicsItem,
              widget: QWidget):

        arrayTransform = self._curve_object._arrayTransform

        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setTransform(arrayTransform, True)

        # p = QPen("orange")
        p = QPen(self._curve_object.color())
        p.setCosmetic(True)
        p.setWidthF(1.0)

        painter.setPen(p)

        painter.drawPath(self._path)
        # for polygon in self._subpathPolygones:
        # for i in range(len(polygon) - 1):
        # painter.drawLine(polygon[i], polygon[i + 1])

        painter.restore()

        # painter.setPen(Qt.NoPen)
        # painter.setBrush(Qt.red)
        # for point in self.points:
        #     painter.drawEllipse(point, 4. / painter.worldTransform().m11(), 4. / painter.worldTransform().m22())


class CurveGraphicsObjectHead(TabletGraphicsObject):

    def __init__(self, parent, curve_object):
        TabletGraphicsObject.__init__(self, parent)

        self._curve_object = curve_object

    def boundingRect(self) -> QRectF:
        br = self._curve_object.boundingRect()

        br.setTop(0.0)
        br.setHeight(0.01)

        return br

    def paint(self,
              painter: QPainter,
              option: QStyleOptionGraphicsItem,
              widget: QWidget):
        p = QPen(self._curve_object.color())
        p.setCosmetic(True)
        p.setWidthF(2.0)
        painter.setPen(p)

        br = self.boundingRect()
        ar = self._curve_object.arrayRect()

        center = br.center()
        leftPoint = QPointF(0.0, center.y())
        rightPoint = QPointF(br.width() - 0.0, center.y())

        t = painter.transform()

        ###

        painter.save()
        painter.resetTransform()

        # Draw scale bar in curve header.

        lp = t.map(leftPoint)
        lp.setX(lp.x() + 2)
        rp = t.map(rightPoint)
        rp.setX(rp.x() - 2)
        line = QLineF(lp, rp)
        line.translate(0, 2)
        painter.drawLine(line)
        # line.translate(0.0, 0.001)

        font = QFont("Monospace")
        font.setStyleHint(QFont.TypeWriter)
        painter.setFont(font)

        fontMetrics = painter.fontMetrics()

        mi, ma = self._curve_object.minMax()

        if self._curve_object.isLogScale():
            mi = 10.0 ** mi
            ma = 10.0 ** ma

        leftValue = "{0:.2f}".format(mi)
        painter.translate(lp)
        painter.drawText(0, 0, leftValue)

        rightValue = "{0:.2f}".format(ma)
        painter.resetTransform()
        painter.translate(rp)
        painter.drawText(br.width() - fontMetrics.width(rightValue), 0, rightValue)

        # ct = self._curve_object._dbCurve.curve_type

        # name = "{} [{}]".format(ct.name, ct.unit.symbol)
        name = f"{self._curve_object._curveName} [{self._curve_object._log.meta.units}]"
        painter.resetTransform()
        painter.translate(t.map(center))
        painter.translate(0, fontMetrics.height() + 3)
        painter.drawText(br.width() - fontMetrics.width(name) / 2, 0, name)

        painter.restore()

        ###
