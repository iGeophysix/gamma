from components.importexport.FamilyAssigner import FamilyAssignerNode
from components.petrophysics.best_log_detection import BestLogDetectionNode
from components.petrophysics.curve_operations import BasicStatisticsNode, LogResolutionNode
from components.petrophysics.normalization import LogNormalizationNode
from components.petrophysics.project_statistics import ProjectStatisticsNode
from components.petrophysics.run_detection import RunDetectionNode
from components.petrophysics.volumetric_model import VolumetricModelSolverNode

NODES = {
    'BasicStatisticsNode': BasicStatisticsNode,
    'LogResolutionNode': LogResolutionNode,
    'RunDetectionNode': RunDetectionNode,
    'FamilyAssignerNode': FamilyAssignerNode,
    'BestLogDetectionNode': BestLogDetectionNode,
    'LogNormalizationNode': LogNormalizationNode,
    'VolumetricModelSolverNode': VolumetricModelSolverNode,
    'ProjectStatisticsNode': ProjectStatisticsNode,
}


class Engine:
    """
    Class that launches EngineNodes
    """

    steps = [
        # {'node': 'BasicStatisticsNode', 'parameters': {}},
        # {'node': 'LogResolutionNode', 'parameters': {}},
        # {'node': 'RunDetectionNode', 'parameters': {}},
        # {'node': 'FamilyAssignerNode', 'parameters': {}},
        {'node': 'BestLogDetectionNode', 'parameters': {}},
        {'node': 'ProjectStatisticsNode', 'parameters': {}},
        {'node': 'LogNormalizationNode', 'parameters': {'lower_quantile': 0.05, 'upper_quantile': 0.95}},
        {'node': 'VolumetricModelSolverNode', 'parameters': {'log_families': ['Gamma Ray', 'Bulk Density', 'Thermal Neutron Porosity'], 'model_components': ['Shale', 'Quartz', 'Calcite', 'Water']}},
    ]
    def start(self):
        for step in self.steps:
            print(f'Starting {step}')
            node = NODES[step['node']]()
            node.run(**step['parameters'])

            print(f'Finished {step}')


if __name__ == '__main__':
    engine = Engine()
    engine.start()
