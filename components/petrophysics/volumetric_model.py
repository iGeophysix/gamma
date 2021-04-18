from scipy.optimize import lsq_linear
import statistics
import numpy as np
import os
import json

import logging
logging.basicConfig()
logger = logging.getLogger('volumetric model')
logger.setLevel(logging.DEBUG)

FLUID_MINERAL_TABLE = os.path.join(os.path.dirname(__file__), 'data', 'FluidMineralConstants.json')


class VolumetricModel():
    '''
    Solver of volumetric mineral and fluid components model.
    '''
    def __init__(self):
        with open(FLUID_MINERAL_TABLE, 'r') as f:
            comp = json.load(f)
        self._family_units = comp['Units']   # units of data table
        del comp['Units']                   # except "Units" item all others are model components
        self._COMPONENTS = comp

    def all_minerals(self) -> tuple:
        '''
        All known minerals
        '''
        return tuple(component for component, info in self._COMPONENTS.items() if not info['Fluid'])

    def all_fluids(self) -> tuple:
        '''
        All known fluids
        '''
        return tuple(component for component, info in self._COMPONENTS.items() if info['Fluid'])

    def inverse(self, logs: dict, components: list) -> dict:
        '''
        Perfoms inversion for provided logs to component volumes.
        :param logs: dict - input logs {'family1': [v1, v2...], ...}
        :param components: list - model components inversion is performed for
        :return: dict - component model {'component1': [v1, v2...], ...}, including 'MISFIT' as a continious quality of the model
        '''
        # TODO: convert log data to self._family_units

        # Equation system:
        # log1_response = component1_log1_response * Vcomponent1 + ... + componentN_log1_response * VcomponentN
        # ...
        # logM_response = component1_logM_response * Vcomponent1 + ... + componentN_logM_response * VcomponentN
        # 1 = Vcomponent1 + ... + VcomponentN
        equation_system_coefficients = []
        equation_system_resulting_logs = []
        useless_logs = set()
        for log_name in logs:
            coef = []   # coefficients of linear equation for the log
            empty_coef = True
            for component in components:
                log_response = self._COMPONENTS[component].get(log_name, 0)
                if log_response != 0:
                    empty_coef = False
                coef.append(log_response)
            if not empty_coef:
                equation_system_coefficients.append(coef)
                equation_system_resulting_logs.append(log_name)
            else:
                useless_logs.add(log_name)
        # the last equation Vcomponent1 + ... + VcomponentN = 1
        equation_system_coefficients.append([1] * len(components))   # component coefficients in equation for summ of all component volumes
        if useless_logs:
            logger.warning(f'the following logs were useless: {useless_logs}')

        log_len = len(next(iter(logs.values())))    # dataset length
        model = {component: np.full(log_len, np.nan) for component in components}
        misfit = {res_log: np.full(log_len, np.nan) for res_log in equation_system_resulting_logs + ['TOTAL']}  # misfit for every input log + total misfit

        if len(equation_system_coefficients) > 1:
            std = {log_name: np.nanstd(data) for log_name, data in logs.items()}
            for row in range(log_len):
                equation_system_results = [logs[log][row] for log in equation_system_resulting_logs] + [1]    # result for every equation in the system
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
                    opt.x *= 1 / sum(opt.x)     # adjust summ of volumes to 1
                    # write down results
                    for n, component in enumerate(components):
                        model[component][row] = opt.x[n]    # main results - componet volumes
                    # misfit calculation
                    misfit_total = 0
                    for n, res_log in enumerate(esrl_clear):
                        mf = abs(opt.fun[n] / std[res_log]) if std[res_log] != 0 else 0     # (model log response - actual log response) / log std
                        misfit[res_log][row] = mf
                        misfit_total += mf
                    misfit['TOTAL'][row] = misfit_total     # overall model misfit at the row
        model['MISFIT'] = misfit
        return model
