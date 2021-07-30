import os
from unittest import TestCase

import numpy as np
from scipy.interpolate import interp1d

from components.database.RedisStorage import RedisStorage
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.importexport.las import import_to_db
from components.petrophysics.log_reconstruction import LogReconstructionNode
from settings import BASE_DIR

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'for_log_restoration')


class TestLogReconstructionNode(TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()
        for filename in os.listdir(PATH_TO_TEST_DATA):
            abs_path = os.path.join(PATH_TO_TEST_DATA, filename)
            well = Well(filename.split("_")[0], new=True)
            ds_name = filename.split('_')[1][:-4]
            well_dataset = WellDataset(well, ds_name, new=True)
            import_to_db(filename=abs_path, well=well, well_dataset=well_dataset)

        # Family Assigner
        log_to_family = {
            "RHOB_NORM": {'family': "Bulk Density", 'display': {'min': 1.7, 'max': 2.8, 'color': (245, 102, 66)}},
            "GR_NORM": {'family': 'Gamma Ray', 'display': {'min': 0, 'max': 150, 'color': (0, 128, 30)}},
            "TNPH_NORM": {'family': "Thermal Neutron Porosity", 'display': {'color': (13, 58, 181)}},
        }
        for well_name in Project.list_wells():
            w = Well(well_name)
            ds = WellDataset(w, 'LQC')
            for log_name, meta in log_to_family.items():
                log = BasicLog(ds.id, log_name)
                log.meta.update(meta)
                log.save()

    def test_run_for_well(self):
        node = LogReconstructionNode()
        well_names_to_predict = ["101", "201", "301", "401", "501", "601"]
        node.run(
            log_families_to_train=['Gamma Ray', 'Thermal Neutron Porosity', ],
            log_families_to_predict=["Bulk Density"],
            model_kwargs={
                'iterations': 50,
                'depth': 12,
                'learning_rate': 0.1,
                'loss_function': 'MAPE',
                'allow_writing_files': False,
                "logging_level": "Silent"
            },
            async_job=True
        )

        for well_name in well_names_to_predict:
            well = Well(well_name)
            ds = WellDataset(well, 'LQC')
            true_rhob = BasicLog(ds.id, 'RHOB_NORM')
            synth_rhob = BasicLog(ds.id, 'RHOB_SYNTH')

            misfit = BasicLog(ds.id, 'RHOB_SYNTH_MISFIT')

            # interp true values
            true_rhob_values_interp = interp1d(true_rhob.values[:, 0],
                                               true_rhob.values[:, 1],
                                               bounds_error=False,
                                               fill_value="extrapolate",
                                               assume_sorted=True,
                                               )(synth_rhob.values[:, 0])
            misfit.values = np.vstack((synth_rhob.values[:, 0], synth_rhob.values[:, 1] - true_rhob_values_interp)).T
            misfit.meta.family = 'Synthetic Bulk Density Misfit'
            misfit.meta.units = ''
            misfit.save()
