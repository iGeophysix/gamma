import numpy as np

from components.domain.Log import BasicLog
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset


def log_normalization(w_wd_log: list[tuple[str, str, str]]) -> [dict, dict]:
    """
    This function normalizes logs from multiple wells and datasets
    Algorithm description is here: https://gammalog.jetbrains.space/p/gr/documents/Petrophysics/a/Normalization-of-Logs-3eTSZ54Vos2Q
    :param w_wd_log: list of tuples with well name, dataset name and log name as string
    :return: dict with normalized data and dict with metadata
    """
    QUANTILES = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95]
    QUANTILES_TO_TIE = (5, 95)
    logs = {}
    # gather all data
    for w, wd, log in w_wd_log:
        well = Well(w)
        dataset = WellDataset(well, wd)
        logs.update({(w, wd, log): BasicLog(dataset.id, log)})

    # define quantiles in each log
    for key, log in logs.items():
        non_null_values = log.values[~np.isnan(log.values[:, 1])]
        log.meta = log.meta | {'quantiles': {q: np.quantile(non_null_values[:, 1], q / 100) for q in QUANTILES}}
        log.save()

    # get median quantiles' values
    median_quantiles = {q: np.median([log.meta['quantiles'][str(q)] for log in logs.values()]) for q in QUANTILES}

    # get sum for each _log quantiles' deviation from median values
    deviations = {}
    for key, log in logs.items():
        deviations[key] = sum([abs(log.meta['quantiles'][str(q)] - median_quantiles[q]) / median_quantiles[q] for q in QUANTILES])

    # define ranking and median quantiles
    rank = sorted(deviations, key=deviations.get)

    def get_quantiles(_logs: dict[BasicLog], _ranked_logs: list, quantiles: tuple = (5, 95)) -> dict[float, float]:
        """
        This internal function averages histograms and returns quantile values
        """
        range_min = min([logs[_log].meta["basic_statistics"]["min_value"] for _log in _ranked_logs])
        range_max = max([logs[_log].meta["basic_statistics"]["max_value"] for _log in _ranked_logs])
        histograms = [np.histogram(logs[_log].values[:, 1], bins=50, range=(range_min, range_max)) for _log in _ranked_logs]
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
    etalon_histogram = np.histogram(etalon_data, bins=20, range=(etalon_data.min(), etalon_data.max()), density=True)
    results = {}
    for key, log in logs.items():
        q_min = log.meta['quantiles'][str(QUANTILES_TO_TIE[0])]
        q_max = log.meta['quantiles'][str(QUANTILES_TO_TIE[1])]
        k = (etalon_q_max - etalon_q_min) / (q_max - q_min)
        new_values = (log[:, 1] - q_min) * k + etalon_q_min

        new_histogram = np.histogram(log.values[:, 1], bins=20, range=(etalon_data.min(), etalon_data.max()), density=True)
        distribution_similarity = sum(abs(new_histogram[0] - etalon_histogram[0]))
        extra_meta = {"normalization": {'difference': distribution_similarity}}

        normalized_log = BasicLog("Normalized", name=log.name) # TODO: un-hardcode dataset name
        normalized_log.values = np.vstack((log.values[:, 0], new_values)).T
        new_meta = log.meta | extra_meta
        del new_meta['quantiles']
        normalized_log.meta = new_meta
        results.update({key: normalized_log})

    return results
