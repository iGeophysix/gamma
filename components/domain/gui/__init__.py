import json
import logging
import os
import sys

from PySide2.QtWidgets import QMenu

from components import ComponentGuiConstructor

from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.rules.src.export_family_assigner_rules import build_family_assigner
from components.importexport.rules.src.export_units_system import build_unit_system
from components.mainwindow.gui import GeoMainWindow
from components.petrophysics.data.src.export_fluid_mineral_constants import build_fluid_mineral_constants
from components.petrophysics.data.src.best_log_tags_assessment import build_best_log_tags_assessment
from settings import BASE_DIR

gamma_logger = logging.getLogger("gamma_logger")


class DomainGui(ComponentGuiConstructor):

    def toolBarActions(self):
        menu = QMenu("Project")
        tablet_action = menu.addAction("Load Common")
        tablet_action.triggered.connect(self._load_common_data)

        return menu

    def dockingWidget(self):
        pass

    def _load_common_data(self):
        gamma_logger.info("Loading Family Properties")
        FamilyProperties.load()
        gamma_logger.info("Loading Best Log Tags Assessment")
        build_best_log_tags_assessment()
        gamma_logger.info("Loading Family Assigner")
        build_family_assigner()
        gamma_logger.info("Loading Fluid Mineral Contants")
        build_fluid_mineral_constants()
        gamma_logger.info("Loading Unit System")
        build_unit_system()


        # load default workflow
        gamma_logger.info("Loading Default Workflow")
        from components.engine.workflow import Workflow
        with open(os.path.join(BASE_DIR, 'default_workflow.json'), 'r') as f:
            s = json.load(f)
        workflow = Workflow('default')
        workflow.set_steps(s)
        workflow.save()

        gamma_logger.info("Finished common data loading")


def initialize_component():
    gui = DomainGui()

    GeoMainWindow().addMenu(gui.toolBarActions())

    mod = sys.modules[__name__]
    mod.gui = gui


if not 'unittest' in sys.modules.keys():
    initialize_component()
