from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.rules.src.export_family_assigner_rules import build_family_assigner
from components.importexport.rules.src.export_units_system import build_unit_system


def load_common_data():
    FamilyProperties.load()
    build_family_assigner()
    build_unit_system()


if __name__ == '__main__':
    load_common_data()
