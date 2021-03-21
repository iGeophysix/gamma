#!/usr/bin/env python3

import sys
import logging

from PySide2.QtCore import QCoreApplication, Qt
from PySide2.QtGui import QFont
from PySide2.QtWidgets import QApplication

import components


def main(profile = False):
    QCoreApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
    QCoreApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

    app = QApplication(sys.argv)

    font = app.font()
    font.setPointSize(9)
    app.setFont(font)


    components.initialize_components()

    gamma_logger = logging.getLogger('gamma_logger')

    if profile:
        app.exec_()
    else:
        sys.exit(app.exec_())


if __name__ == "__main__":
    main()
