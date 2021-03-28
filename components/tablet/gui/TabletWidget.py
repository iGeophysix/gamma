import logging

from PySide2.QtCore import QSize, Qt, QRectF

from PySide2.QtWidgets import (
        QGraphicsView,
        QGraphicsScene,
        QHeaderView,
        QSplitter,
        QVBoxLayout,
        QWidget,
        QScroller,
    )

from PySide2.QtWidgets import QWidget, QGraphicsView

from components.domain.Well import Well
from components.tablet.gui.TabletView import TabletViewHead, TabletViewBody
from components.tablet.gui.WellGroupObject import WellGroupObject
from components.tablet.gui.BodyGraphicsScene import BodyGraphicsScene

# from components.database.dbsession import session

gamma_logger = logging.getLogger("gamma_logger")


class TabletWidget(QWidget):

    def __init__(self):
        QWidget.__init__(self)

        gamma_logger.info("Opening Tablet")

        self._setupGui()

        self._connectSignals()

    def _setupGui(self):
        self.setMinimumSize(QSize(200, 100))

        layout = QVBoxLayout()

        self.head = TabletViewHead()
        self.body = TabletViewBody()

        # TODO: file a bug to Qt. Does not work properly?
        # self.scroller = QScroller.grabGesture(self.view.viewport(),
                                              # QScroller.LeftMouseButtonGesture)

        self.body.horizontalScrollBar().valueChanged.connect(self.head.horizontalScrollBar().setValue)
        self.head.horizontalScrollBar().valueChanged.connect(self.body.horizontalScrollBar().setValue)

        self.body.transformed.connect(self.head.onTransformed)

        self.head_scene = QGraphicsScene()
        self.head.setScene(self.head_scene)

        # self.body_scene = QGraphicsScene()
        well_group = WellGroupObject()
        self.body_scene = BodyGraphicsScene(well_group)
        self.body.setScene(self.body_scene)

        # wells = session.query(Well).all()
        self.head_scene.addItem(well_group.headGraphicsObject())
        self.body_scene.addItem(well_group.bodyGraphicsObject())


        # rect = well.boundingRect()
        # rect.adjust(-rect.width(), -rect.height(),
                     # rect.width(), rect.height())


        # self.scene.setSceneRect(rect)

        s = QSplitter(Qt.Vertical)
        s.addWidget(self.head)
        s.addWidget(self.body)
        s.setStretchFactor(0, 1)
        s.setStretchFactor(1, 5)

        layout.addWidget(s)

        self.setLayout(layout)

    def _connectSignals(self):
        pass
        # self.tree_view.clicked.connect(self.model.onClicked)
        # self.dialog_buttons.accepted.connect(self.onSave)
        # self.dialog_buttons.rejected.connect(self.onCancelled)

    def onSave(self):
        pass
        # self.model.onSave()
        # self.parent().close()

    def onCancelled(self):
        pass
        # self.parent().close()

