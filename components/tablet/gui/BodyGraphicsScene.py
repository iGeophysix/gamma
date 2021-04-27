import json

# from sqlalchemy import or_, and_, func

from PySide2.QtWidgets import (
        QGraphicsScene,
        QGraphicsSceneDragDropEvent
    )
from PySide2.QtCore import QRectF


from components.domain.Well import Well


class BodyGraphicsScene(QGraphicsScene):

    def __init__(self, well_group):
        QGraphicsScene.__init__(self)

        self._well_group = well_group

    def dragEnterEvent(self, event : QGraphicsSceneDragDropEvent):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dragMoveEvent(self, event : QGraphicsSceneDragDropEvent):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event : QGraphicsSceneDragDropEvent):
        s = event.mimeData().text()

        data = json.loads(s)

        self._well_group.loadWells(data)

        br = self.itemsBoundingRect()

        # newRect = QRectF(br.left() - br.width(), br.top(),
                         # br.width() * 3, br.height())

        # self.setSceneRect(br)
