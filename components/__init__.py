from abc import ABC, abstractmethod
import importlib
import logging
import pkgutil
import sys

class ComponentGuiConstructor(ABC):

    @abstractmethod
    def toolBarActions(self):
        pass

    @abstractmethod
    def dockingWidget(self):
        pass


# components = []


def initialize_components():

    gamma_logger = logging.getLogger('gamma_logger')

    # components = []

    def onerror(name):
        print(">>>>>> Error importing module %s" % name)
        type, value, traceback = sys.exc_info()
        print_tb(traceback)
    mod = sys.modules[__name__]

    for sub_module in pkgutil.walk_packages(mod.__path__, mod.__name__ + ".", onerror=onerror):
        # print(sub_module)
        loader, sub_module_name, ispkg = sub_module
        qname = __name__ + "." + sub_module_name

        gamma_logger.info('Importing : {}'.format(qname))
        # components.append(importlib.import_module(qname))

    # for c in components:
        # gamma_logger.info('Initializing : {}'.format(c.__name__))
        # c.initialize_component()

