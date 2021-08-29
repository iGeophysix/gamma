import json
from typing import List, Union, Optional, Iterable, Tuple, Set
from collections import defaultdict

import numpy as np
from scipy.optimize import lsq_linear

from celery_conf import app as celery_app
from components.database.RedisStorage import RedisStorage, FLUID_MINERAL_TABLE_NAME
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.importexport.FamilyProperties import FamilyProperties
from components.importexport.UnitsSystem import UnitsSystem
from components.petrophysics.best_log_detection import score_log_tags
from components.petrophysics.curve_operations import interpolate_to_common_reference
from components.petrophysics.data.src.best_log_tags_assessment import read_best_log_tags_assessment


MODEL_COMPONENT_SET = [
    {
        'core': ('UWater', 'Quartz', 'Shale'),
        'extra': ('Calcite', 'Coal')
    },
    {
        'core': ('UWater', 'Quartz', 'Illite', 'K-Feldspar'),
        'extra': ('Calcite', 'Coal')
    },
    {
        'core': ('UWater', 'Calcite', 'Dolomite')
    },
    {
        'core': ('UWater', 'Calcite', 'Dolomite', 'Anhydrite')
    }
]


class VolumetricModel:
    '''
    Solver of volumetric mineral and fluid components model.
    '''

    def __init__(self):
        s = RedisStorage()
        assert s.object_exists(FLUID_MINERAL_TABLE_NAME), 'Common data is absent'
        comp = json.loads(s.object_get(FLUID_MINERAL_TABLE_NAME))
        self.FAMILY_UNITS = {family: unit for family, unit in comp['_units'].items() if not family.startswith('_')}  # units of data table, excluding special non-component columns
        self.LOG_WEIGHT = {family: weight for family, weight in comp['_weight'].items() if not family.startswith('_')}
        for c in tuple(comp.keys()):
            if c.startswith('_'):
                del comp[c]  # remove special non-component rown
        self._COMPONENTS = comp

    def all_minerals(self) -> Set[str]:
        '''
        All known minerals
        '''
        return set(component for component, info in self._COMPONENTS.items() if not info['_fluid'])

    def all_fluids(self) -> Set[str]:
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
            raise ValueError(f'the following logs are useless: {useless_logs}')
        # Equation system:
        # log1_response = component1_log1_response * Vcomponent1 + ... + componentN_log1_response * VcomponentN
        # ...
        # logM_response = component1_logM_response * Vcomponent1 + ... + componentN_logM_response * VcomponentN
        # 1 = Vcomponent1 + ... + VcomponentN
        equation_system_coefficients = []
        equation_system_resulting_logs = []
        log_scale = []
        family_properties = FamilyProperties()
        for log_fam in logs:
            fp = family_properties[log_fam]
            nmin = fp.get('min')
            nmax = fp.get('max')
            assert None not in (nmin, nmax), f'Min and max limits must be defined for family {log_fam}'
            weight = self.LOG_WEIGHT.get(log_fam, 1)
            ss = ScaleShifter(nmin, nmax, weight)
            logs[log_fam] = ss.normalize(logs[log_fam])
            log_scale.append(ss)
            coef = []  # coefficients of linear equation for the log
            all_coef_empty = True
            for component in components:
                log_response = self._COMPONENTS[component].get(log_fam, 0)
                if log_response != 0:
                    all_coef_empty = False
                coef.append(ss.normalize(log_response))
            if not all_coef_empty:
                equation_system_coefficients.append(coef)
                equation_system_resulting_logs.append(log_fam)
        # the last equation Vcomponent1 + ... + VcomponentN = 1
        equation_system_coefficients.append([1] * len(components))  # component coefficients in equation for summ of all component volumes

        log_len = len(next(iter(logs.values())))  # dataset length
        component_volume = {component: np.full(log_len, 0.) for component in components}
        synthetic_logs = {log_fam: np.full(log_len, np.nan) for log_fam in logs}  # synthetic input logs modeled using resulting volumetric model
        misfit = {res_log: np.full(log_len, np.nan) for res_log in equation_system_resulting_logs + ['TOTAL']}  # misfit for every input log + total misfit

        if len(equation_system_coefficients) > 1:  # if there is at least one input log equation
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
                        log_value = np.sum(np.array(esc_clear[n]) * opt.x)
                        synthetic_logs[res_log][row] = log_scale[n].restore(log_value)
                        # misfit
                        mf = np.abs(opt.fun[n])
                        misfit[res_log][row] = mf  # misfit per model log
                        misfit_total += mf
                    misfit['TOTAL'][row] = misfit_total  # overall model misfit at the row
        res = {'COMPONENT_VOLUME': component_volume, 'MISFIT': misfit, 'FORWARD_MODELED_LOG': synthetic_logs}
        return res


class VolumetricModelDefineModelNode(EngineNode):
    '''
    Select project-wide best VM component set
    '''

    @classmethod
    def run_main(cls, cache: EngineNodeCache, **kwargs) -> None:
        hashes = []
        CORE_MODELS = [model['core'] for model in MODEL_COMPONENT_SET]
        CORE_MODELS_STR = [','.join(sorted(model)) for model in CORE_MODELS]
        min_log_number = max(map(len, CORE_MODELS)) - 1
        tasks = []
        reference_wells = []
        for well_name in Project().list_wells():
            input_logs = pick_input_logs(well_name)
            if len(input_logs) >= min_log_number:
                input_logs_hashes = [log.data_hash for log in input_logs]
                item_hash = cls.item_md5((well_name, sorted(input_logs_hashes)))
                hashes.append(item_hash)
                well_meta = Well(well_name).meta
                item_hash_is_valid = hasattr(well_meta, 'volumetric_model_misfit') and len(set(well_meta['volumetric_model_misfit'].keys()).intersection(CORE_MODELS_STR)) == len(CORE_MODELS_STR)
                if not item_hash_is_valid or item_hash not in cache:
                    tasks.append(celery_app.send_task('tasks.async_calculate_volumetric_model', (well_name, None)))
                reference_wells.append(well_name)
                if len(reference_wells) == 5:  # representative number of wells
                    break
        cache.set(hashes)
        cls.track_progress(tasks)

        model_misfit = defaultdict(list)
        for well_name in reference_wells:
            well_model_stats = Well(well_name).meta.get('volumetric_model_misfit', {})
            for model, misfit in well_model_stats.items():
                model_misfit[model].append(misfit)
        misfit_model = [(np.nanmean(misfits), model) for model, misfits in model_misfit.items() if misfits]
        if misfit_model:
            best_model = sorted(misfit_model)[0][1]
            Project().update_meta({'volumetric_model': best_model})  # update project-wide component set
        else:
            cls.logger.warning('Project has no wells suitable for automatic volumetric model component set selection')

    # @classmethod
    # def item_hash(cls) -> Tuple[str, bool]:
    #     """Get current item hash"""
    #     pass

    # @classmethod
    # def run_async(cls, **kwargs):
    #     well_name = kwargs['well_name']
    #     input_logs_id = kwargs['input_logs_id']


class ScaleShifter():
    '''
    Transforms log data to standard limits and back
    '''
    def __init__(self, init_min: float, init_max: float, weight: float):
        '''
        :param init_min, init_max: common log limits
        :param weight: log weight (target range maximum)
        '''
        self.init_limits = (init_min, init_max)
        self.target_limits = (0, weight)

    def normalize(self, values: Union[float, np.ndarray]):
        '''
        Converts values to standard limits
        '''
        if isinstance(values, (list, tuple)):
            values = np.array(values)
        return (values - self.init_limits[0]) / (self.init_limits[1] - self.init_limits[0]) * (self.target_limits[1] - self.target_limits[0]) + self.target_limits[0]

    def restore(self, values: Union[float, np.ndarray]):
        '''
        Undo normalization
        '''
        return (values - self.target_limits[0]) / (self.target_limits[1] - self.target_limits[0]) * (self.init_limits[1] - self.init_limits[0]) + self.init_limits[0]


class VolumetricModelSolverNode(EngineNode):
    '''
    Solver of volumetric mineral and fluid components model. EngineNode workstep.
    '''

    # class Meta:
    #     name = 'Volumetric model solver'
    #     input = [
    #     ]
    #     output = [
    #     ]

    # class NotEnoughImputLogsError(Exception):
    #     pass

    @classmethod
    def validate_input(cls, model_components: Iterable[str]) -> None:
        '''
        Validate inputs
        :param model_components: list of model component names inversion is performed for (see FluidMineralConstants.json)
        '''
        vm = VolumetricModel()
        unknown_components = set(model_components).difference(vm.all_fluids() | vm.all_minerals())
        if unknown_components:
            raise ValueError(f'"model_components" contains unknown names: {unknown_components}')

    @classmethod
    def run_main(cls, cache: EngineNodeCache, **kwargs) -> None:
        '''
        Volumetric model solver
        Creates set of V[COMPONENT] logs - volume of a component and VMISFIT log - Model Fit Error
        :param model_components: optional list of model component names inversion is performed for (see FluidMineralConstants.json)
        '''
        if 'model_components' in kwargs:
            model_components = kwargs['model_components']
        else:
            p_meta_model = Project().meta.get('volumetric_model')
            model_components = p_meta_model.split(',') if p_meta_model else None

        if model_components:
            cls.validate_input(model_components)

        hashes = []
        cache_hits = 0

        tasks = []
        for well_name in Project().list_wells():

            item_hash, item_hash_is_valid = cls.item_hash(well_name, model_components)
            hashes.append(item_hash)
            if item_hash_is_valid and item_hash in cache:
                cache_hits += 1
                continue

            tasks.append(celery_app.send_task('tasks.async_calculate_volumetric_model',
                                              (well_name, model_components)))

        cache.set(hashes)
        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def item_hash(cls, well_name, model_parameters) -> Tuple[str, bool]:
        """Get current item hash"""
        vm = VolumetricModel()

        # pick input logs
        input_logs_hashes = []
        for family in vm.supported_log_families():
            log = family_best_log(well_name, family, vm.FAMILY_UNITS[family])
            if log is not None:
                input_logs_hashes.append(log.data_hash)
        item_hash = cls.item_md5((well_name, sorted(input_logs_hashes), model_parameters))

        well = Well(well_name)
        lqc_ds = WellDataset(well, 'LQC')
        valid = BasicLog(lqc_ds.id, 'VMISFIT').exists()

        return item_hash, valid

    @classmethod
    def run_async(cls, **kwargs):
        '''
        Runs Volumetrics Model for a given well
        '''

        well_name = kwargs['well_name']
        model_components = kwargs['model_components']

        vm = VolumetricModel()
        family_properties = FamilyProperties()

        # pick input logs
        input_logs = pick_input_logs(well_name)
        if input_logs:
            log_list = ', '.join(f'{log.name} [{log.meta.family}]' for log in input_logs)
            cls.logger.info(f'input logs in well {well_name}: {log_list}')
        else:
            cls.logger.warning(f'well {well_name} has no logs suitable for calculation')
            return

        input_logs = interpolate_to_common_reference(input_logs)

        # convert logs' units
        log_data = {}
        for log in input_logs:
            family_units = vm.FAMILY_UNITS[log.meta.family]
            log_data[log.meta.family] = log.convert_units(family_units)[:, 1]

        # run solver
        base_res = []  # [(avg_misfit, model_components, resuls), ..]
        extra_res = []  # [(avg_misfit, model_components, resuls), ..]
        if model_components is not None:
            # predefined component set
            res = vm.inverse(log_data, model_components)
            avg_misfit = np.nanmean(res['MISFIT']['TOTAL'])
            if not np.isnan(avg_misfit):
                base_res.append((avg_misfit, model_components, res))
        else:
            # find the best component set
            max_component_amount = len(input_logs) + 1
            for component_set in MODEL_COMPONENT_SET:
                core_components = component_set['core']
                if len(core_components) > max_component_amount:
                    continue
                res = vm.inverse(log_data, core_components)
                avg_misfit = np.nanmean(res['MISFIT']['TOTAL'])
                if not np.isnan(avg_misfit):
                    base_res.append((avg_misfit, component_set, res))
            if not base_res:
                cls.logger.error(f'well {well_name} has not enough input logs: {list(map(str, input_logs))}')
                return
            base_res.sort()
            # del base_res[1:]  # drop all base variants except the best
            best_base_misfit, best_base_componet_set, res = base_res[0]
            cls.logger.info(f'well {well_name} best base component set is {best_base_componet_set["core"]} with misfit {best_base_misfit}')

            # calculate additional variants with extra components
            # if 'extra' in best_base_componet_set:
            #     core_components = best_base_componet_set['core']
            #     for extra_component in best_base_componet_set['extra']:
            #         extended_componet_set = (*core_components, extra_component)
            #         if len(extended_componet_set) > max_component_amount:
            #             continue
            #         res = vm.inverse(log_data, extended_componet_set)
            #         avg_misfit = np.nanmean(res['MISFIT']['TOTAL'])
            #         if not np.isnan(avg_misfit):
            #             extra_res.append((avg_misfit, extended_componet_set, res))
            #     extra_res.sort()

        # combine final model
        log_len = len(input_logs[0])
        final_res = {
            'COMPONENT_VOLUME': defaultdict(lambda: np.full(log_len, 0.)),
            'MISFIT': defaultdict(lambda: np.full(log_len, np.nan)),
            'FORWARD_MODELED_LOG': defaultdict(lambda: np.full(log_len, np.nan))
        }
        all_res = (base_res[0], *extra_res)  # one best base + its derivatives
        best_variant_log = [np.nan] * log_len
        for row in range(log_len):
            variant_misfits = [var[2]['MISFIT']['TOTAL'][row] for var in all_res]
            best_variant = sorted((mf, n) for n, mf in enumerate(variant_misfits) if not np.isnan(mf))[0][1]
            best_variant_log[row] = best_variant
            src_res = all_res[best_variant][2]
            for category, logs in src_res.items():
                for log_name, log_values in logs.items():
                    final_res[category][log_name][row] = log_values[row]

        # save results
        if len(base_res) > 1:
            for n, (_, _, res) in enumerate(base_res):
                cls.save_results(vm, family_properties, input_logs, res, str(n))
            # for n, (_, _, res) in enumerate(extra_res):
            #     cls.save_results(vm, family_properties, input_logs, res, '0' + str(n + 1))
        cls.save_results(vm, family_properties, input_logs, final_res)

        log = BasicLog(dataset_id=input_logs[0].dataset_id, log_id='SRC_VARIANT')
        log.values = np.vstack((input_logs[0].values[:, 0], best_variant_log)).T
        log.meta.family = 'Variant Number'
        log.meta.units = 'unitless'
        log.save()

        # save average misfit to the well meta
        well = Well(well_name)
        volumetric_model_misfit = well.meta['volumetric_model_misfit'] if hasattr(well.meta, 'volumetric_model_misfit') else {}
        volumetric_model_misfit.update({','.join(sorted(model_components['core'] if isinstance(model_components, dict) else model_components)): avg_misfit for avg_misfit, model_components, _ in base_res})
        well.update_meta({'volumetric_model_misfit': volumetric_model_misfit})

    @classmethod
    def save_results(cls, vm: VolumetricModel, family_properties: FamilyProperties, input_logs: list, result: dict, version_name: str = '') -> None:
        '''
        Save results to output logs
        '''
        workflow = 'VolumetricModelling'
        dataset = input_logs[0].dataset_id
        log_reference = input_logs[0].values[:, 0]
        if version_name:
            version_name += '_'

        # save components' volumes
        for component_name, data in result['COMPONENT_VOLUME'].items():
            component_family = vm.component_family(component_name)
            log_name = version_name + family_properties[component_family].get('mnemonic', component_family)
            log = BasicLog(dataset_id=dataset, log_id=log_name)
            log.meta.family = component_family
            log.values = np.vstack((log_reference, data)).T
            log.meta.units = 'v/v'
            log.meta.update({'workflow': workflow,
                             'method': 'Deterministic Computation',
                             'display_priority': 1})

            cls.write_history(log=log,
                              input_logs=input_logs)
            log.save()

        # save forward modeled logs
        for n, data in enumerate(result['FORWARD_MODELED_LOG'].values()):
            input_log_family = input_logs[n].meta.family
            log_name = version_name + family_properties[input_log_family].get('mnemonic', input_log_family) + '_FM'
            log = BasicLog(dataset_id=dataset, log_id=log_name)
            log.meta.family = input_log_family
            log.values = np.vstack((log_reference, data)).T
            log.meta.units = input_logs[n].meta.units
            log.meta.add_tags('reconstructed')
            log.meta.update({'workflow': workflow,
                             'method': 'Forward Modelling',
                             'display_priority': 2})
            cls.write_history(log=log,
                              input_logs=input_logs)
            log.save()

        # save misfit
        log_family = 'Model Fit Error'
        log_name = version_name + family_properties[log_family]['mnemonic']
        log = BasicLog(dataset_id=dataset, log_id=log_name)
        log.meta.family = log_family
        log.values = np.vstack((log_reference, result['MISFIT']['TOTAL'])).T
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

        log.meta.append_history({
            'node': cls.name(),
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


def pick_input_logs(well: str) -> List[BasicLog]:
    '''
    Find input logs for VM in a well
    :param well_name: well to search into
    :return: logs
    '''
    vm = VolumetricModel()
    input_logs = []
    for family in vm.supported_log_families():
        log = family_best_log(well, family, vm.FAMILY_UNITS[family])
        if log is not None:
            input_logs.append(log)
    return input_logs
