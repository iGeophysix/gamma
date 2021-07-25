import copy
import json
import logging
import os
import time
from collections.abc import Iterable
from typing import Union, Optional
from datetime import datetime

import numpy as np
from scipy.optimize import lsq_linear

import celery_conf
from celery_conf import app as celery_app, wait_till_completes
from components.database.RedisStorage import RedisStorage

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.UnitsSystem import UnitsSystem
from components.petrophysics.best_log_detection import score_log_tags
from components.petrophysics.data.src.best_log_tags_assessment import read_best_log_tags_assessment
from components.petrophysics.curve_operations import interpolate_log_by_depth
from components.petrophysics.data.src.export_fluid_mineral_constants import FLUID_MINERAL_TABLE

logging.basicConfig()
logger = logging.getLogger('volumetric model')
logger.setLevel(logging.DEBUG)


class VolumetricModel():
    '''
    Solver of volumetric mineral and fluid components model.
    '''

    def __init__(self):
        s = RedisStorage()
        comp = json.loads(s.object_get(FLUID_MINERAL_TABLE))
        self.FAMILY_UNITS = {family: unit for family, unit in comp['Units'].items() if not family.startswith('_')}  # units of data table, excluding system columns
        del comp['Units']  # except "Units" item all others are model components
        self._COMPONENTS = comp

    def all_minerals(self) -> Iterable[str]:
        '''
        All known minerals
        '''
        return set(component for component, info in self._COMPONENTS.items() if not info['_fluid'])

    def all_fluids(self) -> Iterable[str]:
        '''
        All known fluids
        '''
        return set(component for component, info in self._COMPONENTS.items() if info['_fluid'])

    def component_family(self, component: str) -> str:
        '''
        Appropriate family for modeled component volume log
        '''
        return self._COMPONENTS[component]['_output_family']

    def supported_log_families(self) -> Iterable[str]:
        '''
        All supported input log families
        '''
        return set(self.FAMILY_UNITS.keys())

    def inverse(self, logs: dict, components: Iterable[str]) -> dict:
        '''
        Perfoms inversion for provided logs to component volumes.
        :param logs: dict - input logs {'family1': [v1, v2...], ...}
        :param components: list - model components inversion is performed for
        :return: dict of:
        'COMPONENT_VOLUME' - components' volumes
        'FORWARD_MODELED_LOG' - synthetic input logs modeled using resulting volumetric model
        'MISFIT' - model error for every component and 'TOTAL' as overall misfit
        '''
        useless_logs = set(logs).difference_update(self.supported_log_families())
        if useless_logs:
            logger.warning(f'the following logs are useless: {useless_logs}')
        # Equation system:
        # log1_response = component1_log1_response * Vcomponent1 + ... + componentN_log1_response * VcomponentN
        # ...
        # logM_response = component1_logM_response * Vcomponent1 + ... + componentN_logM_response * VcomponentN
        # 1 = Vcomponent1 + ... + VcomponentN
        equation_system_coefficients = []
        equation_system_resulting_logs = []
        for log_name in logs:
            coef = []  # coefficients of linear equation for the log
            empty_coef = True
            for component in components:
                log_response = self._COMPONENTS[component].get(log_name, 0)
                if log_response != 0:
                    empty_coef = False
                coef.append(log_response)
            if not empty_coef:
                equation_system_coefficients.append(coef)
                equation_system_resulting_logs.append(log_name)
        # the last equation Vcomponent1 + ... + VcomponentN = 1
        equation_system_coefficients.append([1] * len(components))  # component coefficients in equation for summ of all component volumes

        log_len = len(next(iter(logs.values())))  # dataset length
        component_volume = {component: np.full(log_len, np.nan) for component in components}
        synthetic_logs = {log_name: np.full(log_len, np.nan) for log_name in logs}  # synthetic input logs modeled using resulting volumetric model
        misfit = {res_log: np.full(log_len, np.nan) for res_log in equation_system_resulting_logs + ['TOTAL']}  # misfit for every input log + total misfit

        if len(equation_system_coefficients) > 1:  # if there is at least one input log equation
            std = {log_name: np.nanstd(data) for log_name, data in logs.items()}
            for row in range(log_len):
                equation_system_results = [logs[log][row] for log in equation_system_resulting_logs] + [1]  # result for every equation in the system
                # remove equations for absent logs
                esc_clear = equation_system_coefficients[:]
                esrl_clear = equation_system_resulting_logs[:]
                esr_clear = equation_system_results[:]
                n = 0
                while n < len(esc_clear):
                    if np.isnan(esr_clear[n]):
                        del esrl_clear[n]
                        del esr_clear[n]
                        del esc_clear[n]
                    else:
                        n += 1
                # run solver
                opt = lsq_linear(esc_clear, esr_clear, [0, 1])  # componet volumes are limited to [0, 1]
                if opt.success:
                    opt.x *= 1 / sum(opt.x)  # adjust summ of volumes to 1
                    # write down results
                    for n, component in enumerate(components):
                        component_volume[component][row] = opt.x[n]  # main results - componet volumes
                    misfit_total = 0
                    for n, res_log in enumerate(esrl_clear):
                        # log forward modeling
                        synthetic_logs[res_log][row] = np.sum(np.array(esc_clear[n]) * opt.x)
                        # misfit calculation
                        mf = abs(opt.fun[n] / std[res_log]) if std[res_log] != 0 else 0  # (model log response - actual log response) / log std
                        misfit[res_log][row] = mf  # misfit per model log
                        misfit_total += mf
                    misfit['TOTAL'][row] = misfit_total  # overall model misfit at the row
        res = {'COMPONENT_VOLUME': component_volume, 'MISFIT': misfit, 'FORWARD_MODELED_LOG': synthetic_logs}
        return res


def interpolate_to_common_reference(logs: Iterable[BasicLog]) -> list[BasicLog]:
    '''
    Interpolates set of logs to a common depths
    :param logs: list of input logs
    :return: list of interpolated logs with a common reference
    '''
    # define smallest depth sampling rate
    step = np.min([log.meta['basic_statistics']['avg_step'] for log in logs])

    # define top and bottom of the common reference
    min_depth = np.min([log.meta['basic_statistics']['min_depth'] for log in logs])
    max_depth = np.max([log.meta['basic_statistics']['max_depth'] for log in logs])

    # interpolate logs
    res_logs = []
    for log in logs:
        int_log = copy.copy(log)
        int_log.values = interpolate_log_by_depth(log.values,
                                                  depth_start=min_depth,
                                                  depth_stop=max_depth,
                                                  depth_step=step)
        res_logs.append(int_log)
    return res_logs


class VolumetricModelSolverNode(EngineNode):
    '''
    Solver of volumetric mineral and fluid components model. EngineNode workstep.
    '''

    class Meta:
        name = 'Volumetric model solver'
        input = [
            # at least one log is required. How to state?
            {'type': BasicLog, 'meta': {'log_family': 'Gamma Ray'}, 'optional': True},
            {'type': BasicLog, 'meta': {'log_family': 'Bulk Density'}, 'optional': True},
            {'type': BasicLog, 'meta': {'log_family': 'Neutron Porosity'}, 'optional': True}
        ]
        output = [
            {'type': BasicLog, 'id': 'VMISFIT', 'meta': {'log_family': 'Model Fit Error', 'method': 'Volumetric model solver'}}
            # plus dynamic set of output logs based on input model_components. How to define?
        ]

    @classmethod
    def validate_input(cls, model_components: Iterable[str]) -> None:
        '''
        Validate inputs
        :param model_components: list of model component names inversion is performed for (see FluidMineralConstants.json)
        '''
        for component in model_components:
            if not isinstance(component, str):
                raise TypeError('all model component names must be str')

    @classmethod
    def run(cls, **kwargs) -> None:
        '''
        Volumetric model solver
        Creates set of V[COMPONENT] logs - volume of a component and VMISFIT log - Model Fit Error
        :param model_components: list of model component names inversion is performed for (see FluidMineralConstants.json)
        '''

        model_components = kwargs['model_components']

        cls.validate_input(model_components)

        tasks = []
        for well_name in Project().list_wells():
            tasks.append(celery_app.send_task('tasks.async_calculate_volumetric_model',
                                              (well_name, model_components)))

        engine_progress = kwargs['engine_progress']
        cls.track_progress(engine_progress, tasks)


    @classmethod
    def run_for_item(cls, **kwargs):
        '''
        Runs Volumetrics Model for a given well
        '''

        well_name = kwargs['well_name']
        model_components = kwargs['model_components']

        vm = VolumetricModel()

        # pick input logs
        input_logs = []
        for family in vm.supported_log_families():
            log = family_best_log(well_name, family, vm.FAMILY_UNITS[family])
            if log is not None:
                input_logs.append(log)
        if input_logs:
            log_list = ', '.join(f'{log.name} [{log.meta.family}]' for log in input_logs)
            logger.info(f'input logs in well {well_name}: {log_list}')
        else:
            logger.warning(f'well {well_name} has no logs suitable for calculation')
            return

        input_logs = interpolate_to_common_reference(input_logs)

        # convert logs' units
        log_data = {}
        for log in input_logs:
            family_units = vm.FAMILY_UNITS[log.meta.family]
            log_data[log.meta.family] = log.convert_units(family_units)[:,1]

        # run solver
        res = vm.inverse(log_data, model_components)

        workflow = 'VolumetricModelling'
        dataset = input_logs[0].dataset_id

        family_properties = FamilyProperties()

        # save components' volumes
        for component_name, data in res['COMPONENT_VOLUME'].items():
            component_family = vm.component_family(component_name)
            log_id = family_properties[component_family].get('mnemonic', component_family)
            log = BasicLog(dataset_id=dataset, log_id=log_id)
            log.meta.family = component_family
            log.values = np.vstack((input_logs[0].values[:, 0], data)).T
            log.meta.units = 'v/v'
            log.meta.update({'workflow': workflow,
                             'method': 'Deterministic Computation',
                             'display_priority': 1})

            cls.write_history(log=log,
                              input_logs=input_logs)
            log.save()

        # save forward modeled logs
        for n, data in enumerate(res['FORWARD_MODELED_LOG'].values()):
            input_log_family = input_logs[n].meta.family
            log_id = family_properties[input_log_family].get('mnemonic', input_log_family) + '_FM'
            log = BasicLog(dataset_id=dataset, log_id=log_id)
            log.meta.family = input_log_family
            log.values = np.vstack((input_logs[0].values[:, 0], data)).T
            log.meta.units = input_logs[n].meta.units
            log.meta.add_tags('reconstructed')
            log.meta.update({'workflow': workflow,
                             'method': 'Forward Modelling',
                             'display_priority': 2})
            cls.write_history(log=log,
                              input_logs=input_logs)
            log.save()
        # save misfit
        log = BasicLog(dataset_id=dataset, log_id='VMISFIT')
        log.meta.family = 'Model Fit Error'
        log.values = np.vstack((input_logs[0].values[:, 0], res['MISFIT']['TOTAL'])).T
        log.meta.units = 'unitless'
        log.meta.update({'workflow': workflow,
                         'method': 'Deterministic Computation',
                         'display_priority': 2})
        cls.write_history(log=log,
                          input_logs=input_logs)
        log.save()


    @classmethod
    def write_history(cls, **kwargs):
        log = kwargs['log']
        input_logs = kwargs['input_logs']

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': {}
                                })


def family_best_log(well_name: str,
                    log_family: Union[str, Iterable[str]],
                    unit: str) -> Optional[BasicLog]:
    '''
    Finds the best log version of the specific family(ies)
    :param well_name: well to search into
    :param log_family: name of a log family or list of acceptable family variants
    :param unit: acceptable log unit (or convertable to)
    :return: best log or None
    '''
    us = UnitsSystem()
    LOG_TAG_ASSESSMENT = read_best_log_tags_assessment()['General log tags']
    best_log = None
    well = Well(well_name)
    dataset = WellDataset(well, 'LQC')
    if dataset.exists:
        wanted_families = (log_family,) if isinstance(log_family, str) else set(log_family)
        best_log_score = float('-inf')
        for log_id in dataset.log_list:
            log = BasicLog(dataset.id, log_id)
            if hasattr(log.meta, 'family') and log.meta.family in wanted_families \
                    and 'reconstructed' not in log.meta.tags \
                    and us.convertable_units(log.meta.units, unit):  # all reconstructed logs are excluded for now
                log_score = score_log_tags(log.meta.tags, LOG_TAG_ASSESSMENT)
                if log_score > best_log_score:
                    best_log_score = log_score
                    best_log = log
    return best_log
