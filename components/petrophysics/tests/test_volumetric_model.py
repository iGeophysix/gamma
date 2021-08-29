import os
import unittest

import numpy as np

from settings import BASE_DIR

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.petrophysics.volumetric_model import VolumetricModel, VolumetricModelSolverNode
from tasks import async_read_las

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
        self.res_component_logs = {'Shale': 'VSHAL',
                                   'Quartz': 'VQUAR',
                                   'Calcite': 'VCALC',
                                   'UWater': 'VUWAT'}

    def test_solver_core(self):
        logs = {}
        logs['Gamma Ray'] = [136.6626, 128.5703, 117.5259, 116.0188, 114.295, np.nan]
        logs['Bulk Density'] = [2.551201, 2.5553, 2.5773, 2.518501, np.nan, np.nan]
        logs['Neutron Porosity'] = [0.2839996, 0.2889999, 0.293, np.nan, np.nan, np.nan]
        expected_res = {'Shale': [0.74, 0.73, 0.71, 0.75, 0.72, 0.25],
                        'Quartz': [0.26, 0.27, 0.0, 0.0, 0.17, 0.25],
                        'Calcite': [0.0, 0.0, 0.29, 0.25, 0.08, 0.25],
                        'UWater': [0.0, 0.0, 0.0, 0.0, 0.03, 0.25]}

        selected_components = expected_res.keys()  # use VolumetricModel.all_minerals() and .all_fluids() to get complete list of possible components
        vm = VolumetricModel()
        model = vm.inverse(logs, selected_components)['COMPONENT_VOLUME']

        # summ of all model component volumes must be 1
        for row in zip(*model.values()):
            self.assertAlmostEqual(sum(row), 1)
        # compare results with the reference
        for component in selected_components:
            self.assertListEqual(list(map(lambda v: round(v, 2), model[component])), expected_res[component])

    def test_solver_engine_node(self):
        module = VolumetricModelSolverNode()
        expected_res = {'Shale': [0.74, 0.73, 0.71, 0.71, 0.68, 0.71],
                        'Quartz': [0.26, 0.27, 0.0, 0.24, 0.29, 0.24],
                        'Calcite': [0.0, 0.0, 0.29, 0.04, 0.0, 0.0],
                        'UWater': [0.0, 0.0, 0.0, 0.0, 0.03, 0.05]}
        model_components = list(expected_res.keys())

        module.start(model_components=model_components)

        for component in model_components:
            log_name = self.res_component_logs[component]
            log = BasicLog(self.wd.id, log_name)
            self.assertTrue(log.exists())
            right_answ = expected_res[component][:6]
            self.assertListEqual(list(map(lambda v: round(v, 2), log.values[:6, 1])), right_answ)
