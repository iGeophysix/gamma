from collections import Counter

import numpy as np
from sklearn.cluster import KMeans

from celery_conf import app as celery_app, check_task_completed
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine_node import EngineNode


def detect_runs_in_well(well: Well, depth_tolerance: float):
    # gather all logs
    logs = []
    features = []
    for datasetname in well.datasets:
        if datasetname == 'LQC':
            continue
        d = WellDataset(well, datasetname)
        for log_id in d.log_list:
            log = BasicLog(d.id, log_id)
            if 'main_depth' in log.meta.tags \
                    or not 'raw' in log.meta.tags \
                    or log.meta.type != 'BasicLog':
                continue
            logs.append(log)
            avg_depth = (log.meta.basic_statistics['max_depth'] + log.meta.basic_statistics['min_depth']) / 2
            log_length = log.meta.basic_statistics['max_depth'] - log.meta.basic_statistics['min_depth']
            features.append((avg_depth, log_length))

    # for each log curve find those that are defined at similar depth (split by runs)
    kmns, run_ids = _define_clusters(depth_tolerance, features)
    if not (kmns or run_ids):
        raise RuntimeError("Run detection failed unexpectedly")

    # define run names
    cluster_centers = kmns.cluster_centers_
    bottoms = (cluster_centers[:, 0] + 0.5 * cluster_centers[:, 1])
    tops = (cluster_centers[:, 0] - 0.5 * cluster_centers[:, 1])
    counter = Counter(run_ids)
    run_names = {i: f"{v}_({np.round(tops[i], 1)}-{np.round(bottoms[i], 1)})" for i, v in counter.items()}
    for i, run_id in enumerate(run_ids):
        log = logs[i]
        log.meta.run = {"value": run_names[run_id],
                        "log_count": counter[run_id],
                        "top": tops[run_id],
                        "bottom": bottoms[run_id],
                        "autocalculated": True}

        log.save()


def _define_clusters(depth_tolerance, features):
    kmns, run_ids = None, None
    for clusters_number in range(1, len(features) + 1):
        kmns = KMeans(clusters_number, n_init=20)
        run_ids = kmns.fit_predict(features)
        if kmns.inertia_ < depth_tolerance ** 2:
            # print("\n".join(f"{m}\t{v}" for m,v in kmns.cluster_centers_)) # for debug only
            break

    return kmns, run_ids


class RunDetectionNode(EngineNode):
    """
    Engine node that detects runs in all wells
    """

    def run(self, depth_tolerance: float = 50):
        """
        Detect pseudo-runs in each well
        :param depth_tolerance:
        :return:
        """
        p = Project()
        tasks = []
        for well_name in p.list_wells():
            result = celery_app.send_task('tasks.async_split_by_runs', (well_name, depth_tolerance))
            tasks.append(result)

        while not all(map(check_task_completed, tasks)):
            continue
