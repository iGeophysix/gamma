import unittest
import numpy as np
import os

from components.petrophysics.volumetric_model import VolumetricModel, VolumetricModelSolverNode
# from components.importexport.FamilyAssigner import FamilyAssigner
from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from tasks import async_read_las, async_get_basic_log_stats

PATH_TO_TEST_DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_data')


class TestVolumetricModel(unittest.TestCase):
    def setUp(self):
        self._s = RedisStorage()
        self._s.flush_db()
        wellname = 'ELAN_TEST'
        self.w = Well(wellname, new=True)
        # loading data
        filename = 'LQC_ELAN_crop.las'
        self.wd = WellDataset(self.w, filename, new=True)
        test_data = os.path.join(PATH_TO_TEST_DATA, filename)
        async_read_las(wellname=self.w.name, datasetname=filename, filename=test_data)
        # assing families
        fam_db = {'GR': 'Gamma Ray', 'RHOB': 'Bulk Density', 'TNPH': 'Thermal Neutron Porosity'}
        for log_name, family in fam_db.items():
            log = BasicLog(self.wd.id, log_name)
            log.meta.family = family
            log.save()
        # getting basic stats
        async_get_basic_log_stats(self.w.name, datasetnames=[filename, ])

        self.res = {'Shale': [0.93, 0.86, 0.75, 0.74, 0.72, 0.25], 'Quartz': [0.07, 0.14, 0.24, 0.14, 0.17, 0.25], 'Calcite': [0.0, 0.0, 0.0, 0.04, 0.08, 0.25], 'Water': [0.0, 0.0, 0.01, 0.08, 0.03, 0.25]}

    def test_solver_core(self):
        logs = {}
        logs['Gamma Ray'] = [136.6626, 128.5703, 117.5259, 116.0188, 114.295, np.nan]
        logs['Bulk Density'] = [2.551201, 2.5553, 2.5773, 2.518501, np.nan, np.nan]
        logs['Thermal Neutron Porosity'] = [0.2839996, 0.2889999, 0.293, np.nan, np.nan, np.nan]

        selected_components = self.res.keys()    # use VolumetricModel.all_minerals() and .all_fluids() to get complete list of possible components
        vm = VolumetricModel()
        model = vm.inverse(logs, selected_components)

        # summ of all model component volumes must be 1
        for row in zip(*(values for component, values in model.items() if component != 'MISFIT')):
            self.assertAlmostEqual(sum(row), 1)
        # compare results with the reference
        for component in selected_components:
            self.assertListEqual(list(map(lambda v: round(v, 2), model[component])), self.res[component])

    def test_solver_engine_node(self):
        module = VolumetricModelSolverNode()
        log_families = ['Gamma Ray', 'Bulk Density', 'Thermal Neutron Porosity']
        model_components = self.res.keys()

        module.run(log_families, model_components)

        for component in model_components:
            log_name = 'VM_' + component
            log = BasicLog(self.wd.id, log_name)
            self.assertTrue(log.exists())
            right_answ = self.res[component][:3]
            self.assertTrue(np.allclose(log.values[:3, 1], right_answ, atol=0.01))
