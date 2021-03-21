import sys
import logging

from PySide2.QtWidgets import (
        QMainWindow,
        QDockWidget,
        QMdiArea,
        QMdiSubWindow,
    )

from PySide2.QtCore import Qt

from components import ComponentGuiConstructor


class GeoMainWindow(QMainWindow):

    _initialized = False
    _instance = None

    def __init__(self):
        if not GeoMainWindow._initialized:
            super().__init__()
            GeoMainWindow._initialized = True

            self.mdi_area = QMdiArea()
            self.setCentralWidget(self.mdi_area)

    def __new__(cls):
        """ Singleton """
        if GeoMainWindow._instance is None:
            GeoMainWindow._instance = super().__new__(cls)
        return GeoMainWindow._instance



    def addToCentral(self, widget, title = ""):
        gamma_logger = logging.getLogger("gamma_logger")

        gamma_logger.debug("Adding to central : {}".format(widget))
        if widget:
            sub_window = self.mdi_area.addSubWindow(widget)
            sub_window.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose);
            sub_window.setWindowTitle(title)
            sub_window.showMaximized()

    def addMenu(self, menu):
        if menu:
            self.menuBar().addMenu(menu)


    def addDockindWidget(self, widget, widget_area):
        if widget:
            dock_widget = QDockWidget()
            dock_widget.setWindowTitle(widget.windowTitle())
            dock_widget.setWidget(widget)
            self.addDockWidget(widget_area, dock_widget)




def initialize_component():
    GeoMainWindow().setWindowTitle("Petrophysics")
    GeoMainWindow().show()
    # GeoMainWindow().showMaximized()

initialize_component()
