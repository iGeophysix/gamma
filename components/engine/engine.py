import datetime

from components.importexport.FamilyAssigner import FamilyAssignerNode
from components.importexport.las_importexport import LasExportNode
from components.petrophysics.best_log_detection import BestLogDetectionNode
from components.petrophysics.curve_operations import BasicStatisticsNode, LogResolutionNode
from components.petrophysics.log_splicing import SpliceLogsNode
from components.petrophysics.normalization import LogNormalizationNode
from components.petrophysics.porosity import PorosityFromDensityNode
from components.petrophysics.project_statistics import ProjectStatisticsNode
from components.petrophysics.run_detection import RunDetectionNode
from components.petrophysics.shale_volume import ShaleVolumeLinearMethodNode, ShaleVolumeLarionovOlderRockNode, ShaleVolumeLarionovTertiaryRockNode
from components.petrophysics.volumetric_model import VolumetricModelSolverNode
from utilities import my_timer

NODES = {
    'BasicStatisticsNode': BasicStatisticsNode,
    'LogResolutionNode': LogResolutionNode,
    'RunDetectionNode': RunDetectionNode,
    'FamilyAssignerNode': FamilyAssignerNode,
    'BestLogDetectionNode': BestLogDetectionNode,
    'LogNormalizationNode': LogNormalizationNode,
    'ProjectStatisticsNode': ProjectStatisticsNode,
    'SpliceLogsNode': SpliceLogsNode,
    'VolumetricModelSolverNode': VolumetricModelSolverNode,
    'ShaleVolumeLinearMethodNode': ShaleVolumeLinearMethodNode,
    'ShaleVolumeLarionovOlderRockNode': ShaleVolumeLarionovOlderRockNode,
    'ShaleVolumeLarionovTertiaryRockNode': ShaleVolumeLarionovTertiaryRockNode,
    'PorosityFromDensityNode': PorosityFromDensityNode,
    'LasExportNode': LasExportNode
}

class Engine:
    """
    Class that launches EngineNodes
    """

    steps = [
        {'node': 'BasicStatisticsNode', 'parameters': {}},
        {'node': 'LogResolutionNode', 'parameters': {}},
        {'node': 'RunDetectionNode', 'parameters': {}},
        {'node': 'FamilyAssignerNode', 'parameters': {}},
        {'node': 'BestLogDetectionNode', 'parameters': {}},
        {'node': 'ProjectStatisticsNode', 'parameters': {}},
        # {'node': 'LogNormalizationNode', 'parameters': {'lower_quantile': 0.05, 'upper_quantile': 0.95}},
        {'node': 'SpliceLogsNode', 'parameters': {}},
        {'node': 'VolumetricModelSolverNode', 'parameters': {'log_families': ['Gamma Ray', 'Bulk Density', 'Neutron Porosity'],
                                                             'model_components': ['Shale', 'Quartz', 'Calcite', 'Water']}},
        {'node': 'ShaleVolumeLinearMethodNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LM'}},
        {'node': 'ShaleVolumeLarionovOlderRockNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LOR'}},
        {'node': 'ShaleVolumeLarionovTertiaryRockNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LTR'}},
        {'node': 'PorosityFromDensityNode', 'parameters': {'rhob_matrix': None, 'rhob_fluid': None, 'output_log_name': 'PHIT_D'}},
        {'node': 'LasExportNode', 'parameters': {}},
    ]

    def start(self):
        for step in self.steps:
            print(f'Starting {step}')
            node = NODES[step['node']]()
            my_timer(node.run)(**step['parameters'])

            print(f'Finished {step}')


if __name__ == '__main__':
    engine = Engine()
    my_timer(engine.start)()
