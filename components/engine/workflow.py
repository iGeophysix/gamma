from dataclasses import dataclass

from components.database.RedisStorage import RedisStorage
from components.importexport.FamilyAssigner import FamilyAssignerNode
from components.importexport.las_importexport import LasExportNode
from components.petrophysics.best_log_detection import BestLogDetectionNode
from components.petrophysics.curve_operations import LogResolutionNode
from components.petrophysics.log_reconstruction import LogReconstructionNode
from components.petrophysics.log_splicing import SpliceLogsNode
from components.petrophysics.normalization import LogNormalizationNode
from components.petrophysics.porosity import PorosityFromDensityNode
from components.petrophysics.project_statistics import ProjectStatisticsNode
from components.petrophysics.run_detection import RunDetectionNode
from components.petrophysics.saturation import SaturationArchieNode
from components.petrophysics.shale_volume import ShaleVolumeLinearMethodNode, ShaleVolumeLarionovOlderRockNode, ShaleVolumeLarionovTertiaryRockNode
from components.petrophysics.volumetric_model import VolumetricModelSolverNode

WORKFLOW_TABLE = 'workflows'

NODES = {
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
    'LogReconstructionNode': LogReconstructionNode,
    'SaturationArchieNode': SaturationArchieNode,
    'LasExportNode': LasExportNode
}


@dataclass
class Workflow:
    """
    A serializable workflow for Engine
    """
    name: str
    steps: list

    def __init__(self, name):
        self.name = name

        self._s = RedisStorage()
        self._load()

    def exists(self):
        """
        Check if workflow exists in DB
        :return:
        """
        return self._s.table_key_exists(WORKFLOW_TABLE, self.name)

    def delete(self):
        """
        Delete workflow from DB
        """
        self._s.table_key_delete(WORKFLOW_TABLE, self.name)

    def validate(self, steps: list = None):
        """
        Validate the workflow has correct structure
        :param steps: steps as list of dicts
        """
        if steps is None:
            steps = self.steps
        for step in steps:
            assert 'node' in step.keys(), f'Node name is not defined in step'
            assert step['node'] in NODES.keys(), f"Node name {step['node']} is not mapped to any EngineNode"
            assert 'parameters' in step.keys(), f'Parameters keys are missing'

    def set_steps(self, steps: list):
        """
        Update workflow steps config
        :param steps: list of dicts
        :return:
        """
        self.validate(steps)
        self.steps = []
        for step in steps:
            self.steps.append({"node": NODES[step['node']], "parameters": step['parameters']})

    def save(self):
        """
        Save workflow state to DB
        :return:
        """
        self._s.table_key_set(WORKFLOW_TABLE, self.name, self.asdict())

    def _load(self):
        """
        Load workflow state from database
        :return:
        """
        if self.exists():
            steps = self._s.table_key_get(WORKFLOW_TABLE, self.name)['steps']
            self.set_steps(steps)
        else:
            self.steps = []

    def asdict(self) -> dict:
        """
        Serialize as dict
        :return:
        """

        nodes_to_str = {v: k for k, v in NODES.items()}

        steps = []
        for step in self.steps:
            steps.append({"node": nodes_to_str[step['node']], "parameters": step['parameters']})

        return {
            'steps': steps,
        }

    def __len__(self):
        return len(self.steps)

    def __getitem__(self, item):
        return self.steps[item] if item is not None else None

    def __iter__(self):
        """
        Method to use workflow in for loop

        for step in Workflow('test'):
            ....

        :return:
        """
        for step in self.steps:
            yield step