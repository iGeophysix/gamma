from PySide2.QtWidgets import QPlainTextEdit
from PySide2.QtCore import Signal, QObject

import logging


class QTextEditLogger(logging.Handler):
    """
    Puts messages from the upstream logger
    into a QPlainTextEdit widget. Thread-safe.
    """
    class SignalProxy(QObject):  # It's workaround
        signal = Signal(str)

    def __init__(self, init_messages=""):
        logging.Handler.__init__(self)
        self.appendText = self.SignalProxy()
        self.widget = QPlainTextEdit()
        self.widget.setWindowTitle("Logger")
        self.widget.setReadOnly(True)
        self.widget.appendPlainText(init_messages)
        self.appendText.signal.connect(self.widget.appendHtml)

        frm = logging.Formatter("%(asctime)s : %(message)s", "%H:%M:%S")
        self.setFormatter(frm)

        self._colors = {
            logging.DEBUG: "dark gray",
            logging.INFO: "black",
            logging.WARNING: "orange",
            logging.ERROR: "red",
            logging.CRITICAL: "brown"
        }

    def emit(self, record):
        msg = self.format(record)
        html_msg = "<font color = \"{}\">{}</font>".format(self._colors[record.levelno], msg)
        self.appendText.signal.emit(html_msg)
