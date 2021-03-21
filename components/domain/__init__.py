import sys
import logging

from components import ComponentGuiConstructor


def initialize_component():

    gui = DomainGui()

    mod = sys.modules[__name__]
    mod.gui = gui


class DomainGui(ComponentGuiConstructor):

    def toolBarActions(self):
        pass

    def dockingWidget(self):
        pass

