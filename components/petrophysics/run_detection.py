import hashlib
import json
from collections import Counter
from datetime import datetime

import numpy as np
from sklearn.cluster import KMeans

from celery_conf import app as celery_app
from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode, EngineNodeCache


def _define_clusters(depth_tolerance, features):
    kmns, run_ids = None, None
    for clusters_number in range(1, len(features) + 1):
        kmns = KMeans(clusters_number, n_init=20)
        run_ids = kmns.fit_predict(features)
        if kmns.inertia_ / len(features) < depth_tolerance ** 2:
            # for debug only
            # print("\n".join(f"{m}\t{v}" for m,v in kmns.cluster_centers_))
            break

    return kmns, run_ids


class RunDetectionNode(EngineNode):
    """
    Engine node that detects runs in all wells
    """

    @classmethod
    def version(cls):
        return 0

    @classmethod
    def run_async(cls, **kwargs):
        '''
        Runs the best log detection for all logs
        belonging to the same RUN and the same Family.
        '''
        wellname = kwargs['wellname']
        depth_tolerance = kwargs['depth_tolerance']

        well = Well(wellname)

        # gather all logs
        logs = []
        features = []
        for datasetname in well.datasets:
            if datasetname == 'LQC':
                continue
            d = WellDataset(well, datasetname)
            for log_id in d.log_list:
                log = BasicLog(d.id, log_id)
                logs.append(log)

                log_statistics = log.meta.basic_statistics

                avg_depth = (log_statistics['max_depth'] + log_statistics['min_depth']) / 2
                log_length = log_statistics['max_depth'] - log_statistics['min_depth']

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

            cls.write_history(log=log)

            log.save()

    @classmethod
    def item_hash(cls, well_name: str) -> tuple[str, bool]:
        """Get item hash to use in cache"""
        w = Well(well_name)
        log_hashes = []
        valid = True
        for ds_id in w.datasets:
            if ds_id == 'LQC':
                continue

            ds = WellDataset(w, ds_id)
            for log_id in ds.log_list:
                log = BasicLog(ds.id, log_id)
                log_hashes.append(log.data_hash)
                if not hasattr(log.meta, 'run'):
                    valid = False

        log_hashes = sorted(log_hashes)
        well_hash = cls.item_md5((well_name, log_hashes))
        return well_hash, valid

    @classmethod
    def run_main(cls, cache: EngineNodeCache, **kwargs):
        """
        Detect pseudo-runs in each well
        :param depth_tolerance:
        :return:
        """
        depth_tolerance = kwargs['depth_tolerance'] if ('depth_tolerance' in kwargs) else 50.0

        p = Project()
        tasks = []
        hashes = []
        cache_hits = 0

        for well_name in p.list_wells():
            item_hash, valid = cls.item_hash(well_name)
            if valid and item_hash in cache:
                hashes.append(item_hash)
                cache_hits += 1
                continue

            result = celery_app.send_task('tasks.async_split_by_runs', (well_name, depth_tolerance))
            tasks.append(result)
            hashes.append(item_hash)

        cache.set(hashes)

        cls.track_progress(tasks, cached=cache_hits)

    @classmethod
    def write_history(cls, **kwargs):
        kwargs['log'].meta.append_history({'node': cls.name(),
                                           'node_version': cls.version(),
                                           'timestamp': datetime.now().isoformat(),
                                           'parent_logs': [],
                                           'parameters': {}
                                           })
