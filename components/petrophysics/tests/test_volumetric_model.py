import os
import unittest

import numpy as np

from components.engine.engine_node import EngineProgress
from settings import BASE_DIR

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.volumetric_model import VolumetricModel, VolumetricModelSolverNode
from tasks import async_read_las
from utilities import my_timer

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'petrophysics')


class TestVolumetricModel(unittest.TestCase):
    def setUp(self):
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = 'ELAN_TEST'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'LQC_ELAN_crop.las'
        self.wd = WellDataset(self.w, 'LQC', new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname='LQC', filename=test_data)
        # assing families
        fam_db = {'GR': 'Gamma Ray', 'RHOB': 'Bulk Density', 'TNPH': 'Neutron Porosity'}
        for log_name, family in fam_db.items():
            log = BasicLog(self.wd.id, log_name)
            log.meta.family = family
            log.save()

        self.res = {'Shale': [0.86, 0.8, 0.73, 0.75, 0.72, 0.25],
                    'Quartz': [0.14, 0.2, 0.12, 0.0, 0.17, 0.25],
                    'Calcite': [0.0, 0.0, 0.15, 0.25, 0.08, 0.25],
                    'UWater': [0.0, 0.0, 0.0, 0.0, 0.03, 0.25]}
        self.res_component_logs = {'Shale': 'VSHAL',
                                   'Quartz': 'VQUAR',
                                   'Calcite': 'VCALC',
                                   'UWater': 'VUWAT'}

    def test_solver_core(self):
        logs = {}
        logs['Gamma Ray'] = [136.6626, 128.5703, 117.5259, 116.0188, 114.295, np.nan]
        logs['Bulk Density'] = [2.551201, 2.5553, 2.5773, 2.518501, np.nan, np.nan]
        logs['Neutron Porosity'] = [0.2839996, 0.2889999, 0.293, np.nan, np.nan, np.nan]

        selected_components = self.res.keys()  # use VolumetricModel.all_minerals() and .all_fluids() to get complete list of possible components
        vm = VolumetricModel()
        model = vm.inverse(logs, selected_components)['COMPONENT_VOLUME']

        # summ of all model component volumes must be 1
        for row in zip(*(values for component, values in model.items())):
            self.assertAlmostEqual(sum(row), 1)
        # compare results with the reference
        for component in selected_components:
            self.assertListEqual(list(map(lambda v: round(v, 2), model[component])), self.res[component])

    def test_solver_engine_node(self):
        module = VolumetricModelSolverNode()
        model_components = list(self.res.keys())

        # module.run(model_components=model_components, )
        my_timer(module.run)(model_components=model_components, )
        my_timer(module.run)(model_components=model_components, )

        for component in model_components:
            log_name = self.res_component_logs[component]
            log = BasicLog(self.wd.id, log_name)
            self.assertTrue(log.exists())
            right_answ = self.res[component][:3]
            self.assertTrue(np.allclose(log.values[:3, 1], right_answ, atol=0.01))
