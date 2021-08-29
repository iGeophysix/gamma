import hashlib
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable, Dict, Tuple, Optional, List

import numpy as np

from celery_conf import app as celery_app
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache
from components.petrophysics.data.src.best_log_tags_assessment import read_best_log_tags_assessment


def score_log_tags(tags: Iterable[str], tags_rank: Dict[str, int]) -> int:
    '''
    Calculates log usefulness by log tags
    :param tags: set of log tags
    :param tags_rank: {tag: score}
    :return: log usefulness score
    '''
    tags = set(map(str.lower, tags))  # remove duplicates, case-insensitive
    rank = sum(tags_rank.get(tag, 0) for tag in tags)
    return rank


class AlgorithmFailure(Exception):
    pass


def get_best_log_for_run_and_family(datasets: Iterable[WellDataset],
                                    family: str,
                                    run_name: str) -> Tuple[str, dict]:
    """
    This method defines best in family log in a dataset withing one run
    :param datasets: list of WellDataset objects to process
    :param family: log family to process e.g. 'Gamma Ray'
    :param run_name: run_id to process e.g. '75_(2600_2800)'
    :return: tuple with name of the best log and dict of new log meta
    """

    # gather all raw logs to process
    logs_meta = {}
    for ds in datasets:
        for log_id in ds.log_list:
            log = BasicLog(ds.id, log_id)
            if 'raw' in log.meta.tags \
                    and 'main_depth' not in log.meta.tags \
                    and log.meta.family == family \
                    and log.meta.run['value'] == run_name:
                logs_meta.update({log_id: log.meta})

    if not logs_meta:
        raise AlgorithmFailure('No logs of this family in this run')

    best_log, new_logs_meta = rank_logs(logs_meta)

    return best_log, new_logs_meta


def rank_logs(logs_meta: Dict[Any, dict], additional_logs_meta: Dict[Any, dict] = None) -> Tuple[Any, dict]:
    '''
    Updates logs' meta with ranking
    :param logs_meta: ranking logs' meta
    :param additional_logs_meta: logs from different runs to make reference statistics representative
    '''
    if not logs_meta:
        raise AlgorithmFailure('No logs were defined as best')
    averages = {('basic_statistics', 'mean'): None, ('basic_statistics', 'stdev'): None, ('log_resolution', 'value'): None}
    for category, metric in averages.keys():
        metric_data = [logs_meta[log_id][category][metric] for log_id in logs_meta.keys()]
        if additional_logs_meta is not None:
            metric_data += [additional_logs_meta[log_id][category][metric] for log_id in additional_logs_meta.keys()]
        if len(logs_meta) != 2:
            val = np.median(metric_data)
        else:
            # it's impossible to choose the best one from two logs. Involve overall project statistics
            log_family = next(iter(logs_meta.values()))['family']  # ranking logs' family
            project_stat = Project().meta['basic_statistics'][log_family]
            if project_stat['number_of_logs'] > 2:
                val = project_stat[metric if category == 'basic_statistics' else category]
            else:
                raise AlgorithmFailure('No logs were defined as best')
        # little trick to avoid division by zero : https://gammalog.jetbrains.space/im/review/2vqWfb3Gfp0m?message=1Jv0001Jv&channel=2pl5Dd4IH2oq
        averages[(category, metric)] = val if val != 0 else val + 1e-16

    average_depth_correction = np.max([logs_meta[log_id]['basic_statistics']['max_depth'] - logs_meta[log_id]['basic_statistics']['min_depth'] for log_id in logs_meta.keys()])

    average_depth_correction = average_depth_correction if average_depth_correction != 0 else average_depth_correction + 1e-16

    new_logs_meta = {log_id: {} for log_id in logs_meta.keys()}
    for log_id, log_meta in logs_meta.items():
        sum_deltas = sum(abs((log_meta[category][metric] - averages[(category, metric)]) / averages[(category, metric)]) for category, metric in averages.keys())
        depth_correction = abs((log_meta['basic_statistics']['max_depth'] - log_meta['basic_statistics']['min_depth']) / average_depth_correction)
        result = sum_deltas / depth_correction
        new_logs_meta[log_id].update({'best_log_detection': {'value': result}})
    # define ranking
    ranking = sorted(new_logs_meta.items(), key=lambda v: (v[1]['best_log_detection']['value'], str(v[0])))
    for i, meta in enumerate(ranking):
        new_logs_meta[meta[0]]['best_log_detection'].update({'rank': i + 1, 'is_best': i == 0})
    best_log = ranking[0][0]
    return best_log, new_logs_meta


def best_rt(rt_candidates: List[BasicLog]) -> Optional[BasicLog]:
    '''
    Returns the best resistivity log
    :param rt_candidates: best resistivity log candidates
    :return: best log or None
    '''
    RESISTIVITY_LOGS_ASSESSMENT = read_best_log_tags_assessment()['Formation Resistivity']
    best_logs = rt_candidates

    if len(best_logs) > 1:
        # round 1: log family
        wrong_family_rank = len(RESISTIVITY_LOGS_ASSESSMENT['family'])
        family_rank = [RESISTIVITY_LOGS_ASSESSMENT['family'].index(log.meta.family) if log.meta.family in RESISTIVITY_LOGS_ASSESSMENT['family'] else wrong_family_rank
                       for log in best_logs]
        best_rank = min(family_rank)
        best_logs = [log for log, rank in zip(best_logs, family_rank) if rank == best_rank and rank != wrong_family_rank]

    if len(best_logs) > 1:
        # round 2: logging service
        source_rank = {src: n for n, src in enumerate(RESISTIVITY_LOGS_ASSESSMENT['logging service'])}
        no_source_rank = len(RESISTIVITY_LOGS_ASSESSMENT['logging service'])
        source_ranked = [source_rank.get(log.meta.family_assigner.get('logging_service'), no_source_rank) for log in best_logs]
        best_rank = min(source_ranked)
        best_logs = [log for log, rank in zip(best_logs, source_ranked) if rank == best_rank]

    if len(best_logs) > 1:
        # round 3: true tag
        true_ranked = [int('true' in log.meta.tags) for log in best_logs]
        best_rank = max(true_ranked)
        best_logs = [log for log, rank in zip(best_logs, true_ranked) if rank == best_rank]

    if len(best_logs) > 1:
        # round 4: DOI value
        DOI_ranked = [log.meta.family_assigner.get('DOI', 0) for log in best_logs]
        best_rank = max(DOI_ranked)
        best_logs = [log for log, rank in zip(best_logs, DOI_ranked) if rank == best_rank]

    if len(best_logs) > 1:
        # round 5: DOI tag
        for tag in RESISTIVITY_LOGS_ASSESSMENT['investigation']:
            filtered_best_logs = [log for log in best_logs if tag in log.meta.tags]
            if filtered_best_logs:
                best_logs = filtered_best_logs
                break

    if len(best_logs) > 1:
        # round 6: vertical_resolution value
        vres_ranked = [log.meta.family_assigner.get('vertical_resolution', np.inf) for log in best_logs]
        best_rank = min(vres_ranked)
        best_logs = [log for log, rank in zip(best_logs, vres_ranked) if rank == best_rank]

    if len(best_logs) > 1:
        # round 7: frequency value
        frequency_ranked = [log.meta.family_assigner.get('frequency', 0) for log in best_logs]
        best_rank = max(frequency_ranked)
        best_logs = [log for log, rank in zip(best_logs, frequency_ranked) if rank == best_rank]

    if len(best_logs) > 1:
        # round 8: general tag assesment
        tags_rank = {log: score_log_tags(log.meta.tags, RESISTIVITY_LOGS_ASSESSMENT['description tags']) for log in best_logs}
        best_logs.sort(key=tags_rank.__getitem__, reverse=True)

    return best_logs[0] if best_logs else None


def intervals_overlap(r1: Tuple[float, float], r2: Tuple[float, float]) -> float:
    '''
    Intervals overlap size
    :param r1, r2: (top, bottom) of interval
    :return: positive overlap thickness if overlaping or negative value of distance between intervals if there is no overlap
    '''
    return min(r1[1], r2[1]) - max(r1[0], r2[0])


def intervals_similarity(run: Tuple[float, float], other_run: Tuple[float, float]) -> float:
    '''
    Estimates expected statistical similarity of two intervals
    :param run: reference interval (top, bottom)
    :param other_run: other interval (top, bottom)
    :return: similarity score
    '''
    # overlapped thickness, bigger is better
    overlap_h = intervals_overlap(run, other_run)
    # not overlapped thickness of the candidate, smaller is better because it brings out-of-interval data
    uniq_h = (other_run[1] - other_run[0]) - overlap_h
    return overlap_h - uniq_h


class BestLogDetectionNode(EngineNode):
    logger = logging.getLogger("BestLogDetectionNode")
    logger.setLevel(logging.INFO)

    @staticmethod
    def validate(log: BasicLog) -> bool:
        return 'raw' in log.meta.tags \
               and 'bad_quality' not in log.meta.tags \
               and hasattr(log.meta, 'family') and log.meta.family \
               and log.meta.family != 'Measured Depth' \
               and hasattr(log.meta, 'basic_statistics') \
               and hasattr(log.meta, 'log_resolution') \
               and hasattr(log.meta, 'run') \
               and 'main_depth' not in log.meta.tags

    @classmethod
    def version(cls):
        return 0

    @classmethod
    def run_async(cls, **kwargs):
        logs = [BasicLog(log[0], log[1]) for log in kwargs['log_paths']]
        log_type = kwargs['log_type']

        if log_type == 'general':
            logs_meta = {log: log.meta for log in logs}
            additional_logs_paths = kwargs['additional_logs_paths']
            if additional_logs_paths is not None:
                additional_logs = [BasicLog(log[0], log[1]) for log in additional_logs_paths]
                additional_logs_meta = {log: log.meta for log in additional_logs}
            else:
                additional_logs_meta = None

            try:
                _, new_meta = rank_logs(logs_meta, additional_logs_meta)
            except AlgorithmFailure:
                BestLogDetectionNode.logger.info(f'No best log in {logs_meta.values()}')
                return

            for log, values in new_meta.items():
                log.meta.update(values)
                log.meta.add_tags('processing')
                cls.write_history(log=log, input_logs=logs, parameters={'log_type': log_type})
                log.save()

        elif log_type == 'resistivity':
            best_log = best_rt(logs)
            if best_log is not None:
                best_log.meta.add_tags('best_rt', 'processing')
                cls.write_history(log=best_log, input_logs=logs, parameters={'log_type': log_type})
                best_log.save()

    @classmethod
    def item_hash(cls, *args) -> Tuple[str, bool]:
        logs_paths, additional_logs_paths = args
        log_hashes = []
        valid = True

        log_paths = logs_paths if additional_logs_paths is None else logs_paths + additional_logs_paths

        for log_path in log_paths:
            log = BasicLog(log_path[0], log_path[1])
            necessary_meta = {k: log.meta[k] for k in ['basic_statistics', 'name', 'description', 'units', ]}
            log_hashes.append(hashlib.md5(json.dumps(necessary_meta).encode()).hexdigest())
            if 'processing' not in log.meta.tags:
                valid = False

        log_hashes = sorted(log_hashes)
        final_hash = hashlib.md5(json.dumps(log_hashes).encode()).hexdigest()
        return final_hash, valid

    @classmethod
    def run_main(cls, cache: EngineNodeCache, **kwargs):
        p = Project()
        tree = p.tree_oop()

        tasks = []
        hashes = []
        cache_hits = 0

        for _, datasets in tree.items():
            run_family_logs_paths = defaultdict(lambda: defaultdict(list))
            run_logs_paths = defaultdict(list)
            for dataset, logs in datasets.items():
                if dataset.name == 'LQC':
                    continue
                # gather run_family_logs in dataset
                for log in logs:
                    if cls.validate(log):
                        run = (log.meta.run['top'], log.meta.run['bottom'])
                        log_path = (log.dataset_id, log._id)
                        run_family_logs_paths[run][log.meta.family].append(log_path)
                        run_logs_paths[run].append(log_path)

            for run, families in run_family_logs_paths.items():
                for family, logs_paths in families.items():
                    additional_logs_paths = None
                    if len(logs_paths) == 2:
                        # logs amount in the run is not enough to choose the best one
                        # get additional logs from nearest runs
                        other_runs = [other_run for other_run in run_family_logs_paths if other_run != run]
                        nearest_runs = sorted(other_runs, key=lambda other_run: intervals_similarity(run, other_run), reverse=True)
                        additional_logs_paths = []
                        for additional_run in nearest_runs:
                            additional_logs_paths += run_family_logs_paths[additional_run][family]
                            if len(logs_paths) + len(additional_logs_paths) > 2:
                                break  # that's enough

                    item_hash, valid = cls.item_hash(logs_paths, additional_logs_paths)
                    hashes.append(item_hash)
                    if valid and item_hash in cache:
                        cache_hits += 1
                        continue

                    tasks.append(celery_app.send_task('tasks.async_detect_best_log', ('general', logs_paths, additional_logs_paths)))

                # resistivity logs processing
                rt_candidates = run_logs_paths[run]

                item_hash, valid = cls.item_hash(rt_candidates, None)
                hashes.append(item_hash)
                if valid and item_hash in cache:
                    cache_hits += 1
                    continue

                tasks.append(celery_app.send_task('tasks.async_detect_best_log', ('resistivity', rt_candidates, None)))

        cache.set(hashes)
        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def write_history(cls, **kwargs):
        log = kwargs['log']
        input_logs = kwargs['input_logs']
        parameters = kwargs['parameters']

        log.meta.append_history({'node': cls.name(),
                                 'node_version': cls.version(),
                                 'timestamp': datetime.now().isoformat(),
                                 'parent_logs': [(log.dataset_id, log.name) for log in input_logs],
                                 'parameters': parameters
                                 })
