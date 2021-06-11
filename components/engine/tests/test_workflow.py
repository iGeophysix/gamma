from unittest import TestCase

from components.database.RedisStorage import RedisStorage
from components.engine.workflow import Workflow


class TestWorkflow(TestCase):
    def setUp(self) -> None:
        s = RedisStorage()
        s.flush_db()
        self.steps = [
            # {'node': 'BasicStatisticsNode', 'parameters': {}},
            {'node': 'LogResolutionNode', 'parameters': {}},
            {'node': 'RunDetectionNode', 'parameters': {}},
            {'node': 'FamilyAssignerNode', 'parameters': {}},
            {'node': 'ProjectStatisticsNode', 'parameters': {}},
            {'node': 'BestLogDetectionNode', 'parameters': {}},
            # {'node': 'LogNormalizationNode', 'parameters': {'lower_quantile': 0.05, 'upper_quantile': 0.95}},
            {'node': 'SpliceLogsNode', 'parameters': {}},
            # LQC zone
            {'node': 'ShaleVolumeLinearMethodNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LM'}},
            {'node': 'ShaleVolumeLarionovOlderRockNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LOR'}},
            {'node': 'ShaleVolumeLarionovTertiaryRockNode', 'parameters': {'gr_matrix': None, 'gr_shale': None, 'output_log_name': 'VSH_GR_LTR'}},
            {'node': 'PorosityFromDensityNode', 'parameters': {'rhob_matrix': None, 'rhob_fluid': None, 'output_log_name': 'PHIT_D'}},
            {'node': 'SaturationArchieNode', 'parameters': {}},
            {'node': 'LogReconstructionNode', 'parameters': {
                'log_families_to_train': ['Gamma Ray', 'Neutron Porosity', ],
                'log_families_to_predict': ["Bulk Density", ],
                'model_kwargs': {
                    'iterations': 50,
                    'depth': 12,
                    'learning_rate': 0.1,
                    'loss_function': 'MAPE',
                    'allow_writing_files': False
                },
            }, },
            {'node': 'LogReconstructionNode', 'parameters': {
                'log_families_to_train': ["Bulk Density", 'Neutron Porosity', ],
                'log_families_to_predict': ['Gamma Ray', ],
                'model_kwargs': {
                    'iterations': 50,
                    'depth': 12,
                    'learning_rate': 0.1,
                    'loss_function': 'MAPE',
                    'allow_writing_files': False
                },
            }, },
            {'node': 'LogReconstructionNode', 'parameters': {
                'log_families_to_train': ['Gamma Ray', "Bulk Density", ],
                'log_families_to_predict': ['Neutron Porosity', ],
                'model_kwargs': {
                    'iterations': 50,
                    'depth': 12,
                    'learning_rate': 0.1,
                    'loss_function': 'MAPE',
                    'allow_writing_files': False
                },
            }, },
            {'node': 'VolumetricModelSolverNode', 'parameters': {'model_components': ['Shale', 'Quartz', 'Calcite', 'UWater']}},
            {'node': 'LasExportNode', 'parameters': {'destination': 'LQC'}},

        ]

    def test_exists(self):
        workflow = Workflow("test")
        self.assertFalse(workflow.exists())
        workflow.save()
        self.assertTrue(workflow.exists())

    def test_delete(self):
        workflow = Workflow("test")
        workflow.save()
        self.assertTrue(workflow.exists())
        workflow.delete()
        self.assertFalse(workflow.exists())

    def test_validate(self):
        Workflow("test").validate(self.steps)
        Workflow("test").validate([{"node": "FamilyAssignerNode", "parameters": {}}])
        Workflow("test").validate([])

        with self.assertRaises(AssertionError):
            Workflow("test").validate([{"node": "NON existent node", "parameters": {}}])
        with self.assertRaises(AssertionError):
            Workflow("test").validate([{"parameters": {}}])
        with self.assertRaises(AssertionError):
            Workflow("test").validate([{"node": "NON existent node", }])

    def test_save_and_load_to_db(self):
        workflow = Workflow("test")
        workflow.delete()
        workflow.set_steps(self.steps)

        self.assertEqual(self.steps, workflow.asdict()['steps'])

