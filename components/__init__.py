from abc import ABC, abstractmethod
import importlib
import logging
import pkgutil

class ComponentGuiConstructor(ABC):

    @abstractmethod
    def toolBarActions(self):
        pass

    @abstractmethod
    def dockingWidget(self):
        pass


components = []


def initialize_components():

    gamma_logger = logging.getLogger('gamma_logger')

    components = []

    for sub_module in pkgutil.walk_packages([__name__]):
        loader, sub_module_name, ispkg = sub_module
        qname = __name__ + "." + sub_module_name

        gamma_logger.info('Importing : {}'.format(qname))
        components.append(importlib.import_module(qname))

    for c in components:
        gamma_logger.info('Initializing : {}'.format(c.__name__))
        c.initialize_component()

