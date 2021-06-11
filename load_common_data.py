import json
import os

from components.engine.workflow import Workflow
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.rules.src.export_family_assigner_rules import build_family_assigner
from components.importexport.rules.src.export_units_system import build_unit_system
from components.petrophysics.data.src.export_fluid_mineral_constants import build_fluid_mineral_constants
from settings import BASE_DIR


def load_common_data():
    FamilyProperties.load()
    build_family_assigner()
    build_unit_system()
    build_fluid_mineral_constants()

    # load default workflow
    with open(os.path.join(BASE_DIR, 'default_workflow.json'), 'r') as f:
        s = json.load(f)
    workflow = Workflow('default')
    workflow.set_steps(s)
    workflow.save()


if __name__ == '__main__':
    load_common_data()
