import unittest
from components.petrophysics.volumetric_model import VolumetricModel
import numpy as np


class TestVolumetricModel(unittest.TestCase):
    def setUp(self):
        self.vm = VolumetricModel()

    def test_solver(self):
        logs = {}
        logs['Gamma Ray'] = [136.6626, 128.5703, 117.5259, 116.0188, 114.295, np.nan]
        logs['Bulk Density'] = [2.551201, 2.5553, 2.5773, 2.518501, np.nan, np.nan]
        logs['Thermal Neutron Porosity'] = [0.2839996, 0.2889999, 0.293, np.nan, np.nan, np.nan]

        res = {'Shale': [0.93, 0.86, 0.75, 0.74, 0.72, 0.25], 'Quartz': [0.07, 0.14, 0.24, 0.14, 0.17, 0.25], 'Calcite': [0.0, 0.0, 0.0, 0.04, 0.08, 0.25], 'Water': [0.0, 0.0, 0.01, 0.08, 0.03, 0.25]}
        selected_components = res.keys()    # use VolumetricModel.all_minerals() and .all_fluids() to get complete list of possible components

        model = self.vm.inverse(logs, selected_components)

        # summ of all model component volumes must be 1
        for row in zip(*(values for component, values in model.items() if component != 'MISFIT')):
            self.assertAlmostEqual(sum(row), 1)
        # compare results with the reference
        for component in selected_components:
            self.assertListEqual(list(map(lambda v: round(v, 2), model[component])), res[component])
