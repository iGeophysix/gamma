import json

# from sqlalchemy import or_, and_, func

from PySide2.QtWidgets import (
        QGraphicsScene,
        QGraphicsSceneDragDropEvent
    )
from PySide2.QtCore import QRectF


from components.domain.Well import Well# , WellProperty
# from components.database.dbsession import session


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
        wellList = json.loads(s)

        dbWells = [Well(name) for name in wellList]

        # dbWells = session.query(Well).\
                # join(WellProperty).\
                # filter(and_(WellProperty.name == "WELL",
                            # WellProperty.value.in_(wellList)))

        self._well_group.loadWells(dbWells)

        br = self.itemsBoundingRect()

        # newRect = QRectF(br.left() - br.width(), br.top(),
                         # br.width() * 3, br.height())

        self.setSceneRect(br)
