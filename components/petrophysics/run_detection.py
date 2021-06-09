from collections import Counter

from datetime import datetime
from sklearn.cluster import KMeans
import numpy as np

from celery_conf import app as celery_app, wait_till_completes

from components.domain.Log import BasicLog
from components.domain.Project import Project
from components.domain.Well import Well
from components.domain.WellDataset import WellDataset
from components.engine.engine_node import EngineNode


def _define_clusters(depth_tolerance, features):
    kmns, run_ids = None, None
    for clusters_number in range(1, len(features) + 1):
        kmns = KMeans(clusters_number, n_init=20)
        run_ids = kmns.fit_predict(features)
        if kmns.inertia_  / len(features) < depth_tolerance ** 2:
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
    def run_for_item(cls, **kwargs):
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
    def run(cls, **kwargs):
        """
        Detect pseudo-runs in each well
        :param depth_tolerance:
        :return:
        """
        depth_tolerance = kwargs['depth_tolerance'] if ('depth_tolerance' in kwargs) else 50.0;
        async_job = kwargs['async_job'] if ('async_job' in kwargs) else True

        p = Project()
        tasks = []
        if not async_job:
            for well_name in p.list_wells():
                detect_runs_in_well(Well(well_name), depth_tolerance)
        else:
            for well_name in p.list_wells():
                result = celery_app.send_task('tasks.async_split_by_runs', (well_name, depth_tolerance))
                tasks.append(result)

            wait_till_completes(tasks)

    @classmethod
    def write_history(cls, **kwargs):
        kwargs['log'].meta.append_history({ 'node': cls.name(),
                                            'node_version': cls.version(),
                                            'timestamp': datetime.now().isoformat(),
                                            'parent_logs': [],
                                            'parameters': {}
                                          })

if __name__ == '__main__':
    RunDetectionNode.run(async_job=False)
