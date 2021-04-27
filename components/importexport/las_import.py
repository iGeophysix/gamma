from typing import Iterable

from components.engine_node import EngineNode
from components.importexport.las import import_to_db


class LasImportNode(EngineNode):
    """
    Engine node to import files
    """

    def run(self, paths: Iterable[str]):
        """
        :param paths: Absolute paths to files, accessible to task reader
        """
        for path in paths:
            import_to_db(filename=path)
