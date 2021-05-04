from collections import defaultdict

import numpy as np

from components.domain.Project import Project
from components.engine_node import EngineNode


class ProjectStatisticsNode(EngineNode):
    """
    Node to calculate project's data statistics.
    """

    @classmethod
    def run(cls):
        p = Project()
        tree = p.tree_oop()

        # split logs by families
        logs_by_families = defaultdict(list)
        for well in tree.values():
            for dataset in well.values():
                for log in dataset:
                    logs_by_families[log.meta.family].append(log)

        stats_by_family = defaultdict(dict)
        for family, logs in logs_by_families.items():
            stats_by_family[family] = {
                'mean': np.nanmean([log.meta.basic_statistics['mean'] for log in logs]),
                'gmean': np.nanmean([log.meta.basic_statistics['gmean'] for log in logs]),
                'stdev': np.nanmean([log.meta.basic_statistics['stdev'] for log in logs]),
                'log_resolution': np.nanmean([log.meta.log_resolution['value'] for log in logs]),
            }

        p.update_meta({'basic_statistics': stats_by_family})


if __name__ == '__main__':
    node = ProjectStatisticsNode()
    node.run()
