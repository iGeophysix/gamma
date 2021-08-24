import logging

from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.rules.src.export_family_assigner_rules import build_family_assigner
from components.importexport.rules.src.export_units_system import build_units_system
from components.petrophysics.data.src.export_fluid_mineral_constants import build_fluid_mineral_constants
from components.petrophysics.data.src.best_log_tags_assessment import build_best_log_tags_assessment

gamma_logger = logging.getLogger("gamma_logger")


def load_common_data():
    gamma_logger.info("Loading Family Properties")
    FamilyProperties.build_family_properties()

    gamma_logger.info("Loading Best Log Tags Assessment")
    build_best_log_tags_assessment()

    gamma_logger.info("Loading Family Assigner")
    build_family_assigner()

    gamma_logger.info("Loading Fluid Mineral Contants")
    build_fluid_mineral_constants()

    gamma_logger.info("Loading Units System")
    build_units_system()

    gamma_logger.info("Loading Default Workflow")
    from components.engine.workflow import build_default_workflow  # moved here to solve GUI import issue
    build_default_workflow()

    gamma_logger.info("Finished common data loading")


if __name__ == '__main__':
    load_common_data()
