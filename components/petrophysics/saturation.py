import logging

import numpy as np

import celery_conf
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.importexport.FamilyProperties import FamilyProperties


class SaturationArchieNode(EngineNode):
    """
    Saturation via Archie method
    """
    @classmethod
    def version(cls):
        return 1

    @classmethod
    def run_async(cls, well_name: str, a: float, m: float, n: float, rw: float, output_log_name: str = 'SW_AR') -> None:
        """
        Function to calculate Saturation via Archie method in a well
        :param well_name: name of well to process
        :param a: tortuosity exponent (unitless)
        :param m: cementation exponent (unitless)
        :param n: saturation exponent (unitless)
        :param rw: effective porosity (fractional)
        :param output_log_name: default, SW_AR
        """
        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')

        # get best effective porosity log
        phit_logs = dataset.get_log_list(family='Total Porosity')
        if phit_logs:
            # todo insert best log selection
            phit = BasicLog(dataset.id, phit_logs[0])
            phit.values = phit.convert_units('v/v')
        else:
            cls.logger.info(f"No Effective porosity log in well {well_name}")
            return

        # get best formation resistivity log
        resf_logs = dataset.get_log_list(family='Formation Resistivity')
        if resf_logs:
            # todo insert best log selection
            resf = BasicLog(dataset.id, resf_logs[0])
            resf.values = resf.convert_units('ohmm')
        else:
            cls.logger.info(f"No True Resistivity log in well {well_name}")
            return

        # get common depth reference
        md_min = min(phit.meta.basic_statistics['min_depth'], resf.meta.basic_statistics['min_depth'])
        md_max = min(phit.meta.basic_statistics['max_depth'], resf.meta.basic_statistics['max_depth'])
        md_step_min = min(phit.meta.basic_statistics['avg_step'], resf.meta.basic_statistics['avg_step'])
        new_md = np.arange(start=md_min, stop=md_max, step=md_step_min)

        phit_values = phit.interpolate(new_md)
        resf_values = resf.interpolate(new_md)

        # calculate value
        sw_values = (a * rw / (phit_values[:, 1] ** m) / resf_values[:, 1]) ** (1 / n)

        # save unclipped log
        sw_ar_unclipped = cls._prepare_output_log(dataset.id, 'Water Saturation', output_log_name + '_UNCL', phit_values[:, 0], sw_values)
        sw_ar_unclipped.meta.add_tags('unclipped')
        sw_ar_unclipped.save()

        # save clipped log
        sw_values_clipped = np.clip(sw_values, 0, 1)
        sw_ar = cls._prepare_output_log(dataset.id, 'Water Saturation', output_log_name, phit_values[:, 0], sw_values_clipped)
        sw_ar.save()

        sh_values = 1 - sw_values_clipped
        sh_ar = cls._prepare_output_log(dataset.id, 'Hydrocarbon Saturation', 'SH_AR', phit_values[:, 0], sh_values)
        sh_ar.save()

        bvw_values = phit_values[:, 1] * sw_values_clipped
        bvw_ar = cls._prepare_output_log(dataset.id, 'Bulk Water Volume', 'BVW_AR', phit_values[:, 0], bvw_values)
        bvw_ar.save()

    @staticmethod
    def _prepare_output_log(dataset_id, output_family, output_log_name, md_ref, sw_values_clipped):
        family_meta = FamilyProperties()[output_family]

        log = BasicLog(dataset_id, output_log_name)
        log.values = np.vstack((md_ref, sw_values_clipped)).T

        log.meta.family = output_family
        log.meta.units = family_meta.get('unit', 'v/v')
        # log.meta.name = family_meta.get('mnemonic', output_log_name)
        log.meta.workstep = 'Saturation'
        log.meta.method = 'Archie'
        return log

    @classmethod
    def item_hash(cls, well_name, a, m, n, rw, output_log_name) -> tuple[str, bool]:
        """Get current item hash"""
        well = Well(well_name)
        dataset = WellDataset(well, 'LQC')
        log_hashes = []

        for log_name in dataset.get_log_list(family='Total Porosity'):
            log = BasicLog(dataset.id, log_name)
            if 'spliced' in log.meta.tags:
                log_hashes.append(log.data_hash)

        item_hash = cls.item_md5((well_name, sorted(log_hashes), a, m, n, rw, output_log_name))

        valid = BasicLog(dataset.id, output_log_name).exists()

        return item_hash, valid

    @classmethod
    def run_main(cls, cache: EngineNodeCache, **kwargs):
        """
        :param a: tortuousity exponent (unitless)
        :param m: cementation exponent (unitless)
        :param n: saturation exponent (unitless)
        :param rw: water resistivity (ohmm)
        :param output_log_name: default SW_AR
        :param async_job: default True
        :return:
        """
        a = kwargs.get('a', 1)
        m = kwargs.get('m', 2)
        n = kwargs.get('n', 2)
        rw = kwargs.get('rw', 0.03)
        output_log_name = kwargs.get('output_log_name', 'SW_AR')
        async_job = kwargs.get('async_job', True)

        p = Project()
        well_names = p.list_wells()

        if async_job:
            tasks = []
            hashes = []
            cache_hits = 0

            for well_name in well_names:

                item_hash, item_hash_is_valid = cls.item_hash(well_name, a, m, n, rw, output_log_name)
                hashes.append(item_hash)
                if item_hash_is_valid and item_hash in cache:
                    cache_hits += 1
                    continue
                tasks.append(celery_conf.app.send_task('tasks.async_saturation_archie', (well_name, a, m, n, rw, output_log_name)))

            cache.set(hashes)
            cls.track_progress(tasks, cached=cache_hits)

        else:
            for well_name in well_names:
                cls.run_async(well_name, a, m, n, rw, output_log_name)
