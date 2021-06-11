import warnings
from collections import defaultdict
from typing import Iterable

import numpy as np

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset, get_dataset_by_id
from components.engine.engine_node import EngineNode

QUANTILES = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
QUANTILES_TO_TIE = (5, 95)


def _normalize_logs(logs: dict[tuple, BasicLog]) -> dict[tuple, BasicLog]:
    # define quantiles in each log
    for key, log in logs.items():
        non_null_values = log.values[~np.isnan(log.values[:, 1])]
        log.meta.update({'quantiles': {q: np.quantile(non_null_values[:, 1], q / 100) for q in QUANTILES}})
    # get median quantiles' values
    median_quantiles = {q: np.median([log.meta['quantiles'][q] for log in logs.values()]) for q in QUANTILES}
    # get sum for each _log quantiles' deviation from median values
    deviations = {}
    for key, log in logs.items():
        deviations[key] = sum([abs(log.meta['quantiles'][q] - median_quantiles[q]) / median_quantiles[q] for q in QUANTILES])
    # define ranking and median quantiles
    rank = sorted(deviations, key=deviations.get)

    def get_quantiles(_logs: dict[tuple, BasicLog], _ranked_logs: list, quantiles: tuple = (5, 95)) -> dict[float, float]:
        """
        This internal function averages histograms and returns quantile values
        """
        range_min = min([_logs[_log].meta["basic_statistics"]["min_value"] for _log in _ranked_logs])
        range_max = max([_logs[_log].meta["basic_statistics"]["max_value"] for _log in _ranked_logs])

        histograms = [np.histogram(_logs[_log].values[:, 1], bins=50, range=(range_min, range_max)) for _log in _ranked_logs]

        mean_histogram = np.mean([h[0] for h in histograms], axis=0)
        cs = np.cumsum(mean_histogram) / np.sum(mean_histogram) * 100
        return {q: histograms[0][1][len(cs[cs <= q])] for q in quantiles}

    if len(rank) >= 15:
        ranked_logs = rank[0:3]  # take 3 best ranked curves and get mean of them
    elif len(rank) >= 8:
        ranked_logs = rank[0:2]  # take 2 best ranked curves and get mean of them
    else:
        ranked_logs = rank[0:1]  # few logs - get only one best _log
    etalon_quantiles = get_quantiles(logs, ranked_logs, quantiles=QUANTILES_TO_TIE)
    etalon_q_min, etalon_q_max = etalon_quantiles.values()
    etalon_data = logs[rank[0]][:, 1]
    etalon_histogram = np.histogram(etalon_data, bins=20, range=(np.nanmin(etalon_data), np.nanmax(etalon_data)), density=True)
    results = {}
    for key, log in logs.items():
        q_min = log.meta['quantiles'][QUANTILES_TO_TIE[0]]
        q_max = log.meta['quantiles'][QUANTILES_TO_TIE[1]]
        k = (etalon_q_max - etalon_q_min) / (q_max - q_min)
        new_values = (log[:, 1] - q_min) * k + etalon_q_min

        new_histogram = np.histogram(new_values, bins=20, range=(np.nanmin(etalon_data), np.nanmax(etalon_data)), density=True)
        distribution_similarity = sum(abs(new_histogram[0] - etalon_histogram[0]))
        extra_meta = {"normalization": {'difference': distribution_similarity}}

        normalized_log = BasicLog(dataset_id=log.dataset_id, log_id=log.name)
        normalized_log.values = np.vstack((log.values[:, 0], new_values)).T
        new_meta = log.meta.asdict() | extra_meta
        del new_meta['quantiles']
        normalized_log.meta = new_meta
        results.update({key: normalized_log})
    return results


class LogNormalizationNode(EngineNode):
    """
    This node adjusts logs distributions by two quantiles (e.g. Q5 and Q95 will be the same for all logs)
    Algorithm description is here: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/a/Normalization-of-Logs-3eTSZ54Vos2Q
    """

    class Meta:
        name = 'Log Normalization'
        input = None
        output = None

    @classmethod
    def validate_input(cls, logs: Iterable[BasicLog]) -> None:
        """
        Check if input is valid for this. If all is OK - return None, else raise an exception
        """
        pass

    @classmethod
    def run(cls, lower_quantile: float = 0.05, upper_quantile: float = 0.95) -> None:
        """
        Run calculations
        :param lower_quantile: lower quantile value to tie to. Default: 0.05
        :param upper_quantile: upper quantile value to tie to. Default: 0.95
        """
        p = Project()
        well_names = p.list_wells()
        logs_by_families = defaultdict(list)
        for well_name in well_names:
            well = Well(well_name)
            dataset_names = well.datasets
            for dataset_name in dataset_names:
                if dataset_name == 'LQC':
                    continue
                dataset = WellDataset(well, dataset_name)

                # gather runs in dataset
                for log_id in dataset.log_list:
                    log = BasicLog(dataset.id, log_id)
                    if not hasattr(log.meta, 'family'):
                        continue
                    logs_by_families[log.meta.family].append(log)

        for family, logs in logs_by_families.items():
            input_for_normalization = {}
            for log in logs:
                dataset = get_dataset_by_id(log.dataset_id)
                well = Well(dataset.well)
                input_for_normalization.update({(well.name, dataset.name, log.name): log})

            results = _normalize_logs(input_for_normalization)
            for w_ds_l, log in results.items():
                well_name, dataset_name, log_name = w_ds_l
                well = Well(well_name)
                dataset = WellDataset(well, 'LQC')
                if not dataset.exists:
                    dataset = WellDataset(well, 'LQC', new=True)
                log.dataset_id = dataset.id
                log.meta.add_tags('Normalized')
                log.save()
