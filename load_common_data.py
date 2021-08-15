import json
import os
import logging

from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.rules.src.export_family_assigner_rules import build_family_assigner
from components.importexport.rules.src.export_units_system import build_unit_system
from components.petrophysics.data.src.export_fluid_mineral_constants import build_fluid_mineral_constants
from components.petrophysics.data.src.best_log_tags_assessment import build_best_log_tags_assessment
from settings import BASE_DIR

gamma_logger = logging.getLogger("gamma_logger")


def load_common_data():
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

    gamma_logger.info("Loading Default Workflow")
    from components.engine.workflow import Workflow  # moved here to solve GUI import issue
    with open(os.path.join(BASE_DIR, 'default_workflow.json'), 'r') as f:
        s = json.load(f)
    workflow = Workflow('default')
    workflow.set_steps(s)
    workflow.save()

    gamma_logger.info("Finished common data loading")


if __name__ == '__main__':
    load_common_data()
