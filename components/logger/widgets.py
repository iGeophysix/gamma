from PySide2.QtWidgets import QWidget, QPlainTextEdit, QVBoxLayout

import logging

class QTextEditLogger(logging.Handler):
    """
    Puts messages from the upstream logger into
    a QPlainTextEdit widget
    """

    def __init__(self, logger, init_messages = ""):
        super().__init__()
        self.widget = QPlainTextEdit()
        self.widget.setWindowTitle("Logger")
        self.widget.setReadOnly(True)
        self.widget.appendPlainText(init_messages)

        frm = logging.Formatter("%(asctime)s : %(message)s", "%H:%M:%S")
        self.setFormatter(frm)

        logger.addHandler(self)

        self._colors = { logging.DEBUG : "blue",
                         logging.INFO  : "black",
                         logging.WARNING : "orange",
                         logging.ERROR   : "red",
                         logging.CRITICAL : "brown" }

    def emit(self, record):
        msg = self.format(record)
        self.widget.appendHtml("<font color = \"{}\">{}</font>".format(self._colors[record.levelno], msg))
