import io
import logging
import sys

from PySide2.QtWidgets import QAction, QMenu
from PySide2.QtCore import Qt

from components import ComponentGuiConstructor
from components.logger.gui.widgets import QTextEditLogger
from components.mainwindow.gui import GeoMainWindow


class LoggerGui(ComponentGuiConstructor):

    def __init__(self):

        log_capture_string = io.StringIO()
        ch = logging.StreamHandler(log_capture_string)
        ch.setLevel(logging.DEBUG)
        frm = logging.Formatter("%(asctime)s : %(message)s", "%H:%M:%S")
        ch.setFormatter(frm)

        gamma_logger = logging.getLogger('gamma_logger')
        gamma_logger.addHandler(ch)

        self.text_edit_logger = QTextEditLogger(gamma_logger, log_capture_string.getvalue())

    def toolBarActions(self):
        menu = QMenu("Logger")
        menu.addAction("Show logger window")

        return menu

    def dockingWidget(self):
        return self.text_edit_logger.widget

def initialize_component():
    gui = LoggerGui()

    GeoMainWindow().addMenu(gui.toolBarActions())
    GeoMainWindow().addDockindWidget(gui.dockingWidget(), Qt.RightDockWidgetArea)
    # Qt.BottomDockWidgetArea

    mod = sys.modules[__name__]
    mod.gui = gui


if not 'unittest' in sys.modules.keys():
    initialize_component()
