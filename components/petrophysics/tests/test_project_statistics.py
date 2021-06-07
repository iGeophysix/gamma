import os
from unittest import TestCase

from components.database.RedisStorage import RedisStorage
from components.domain.Project import Project
from components.importexport.FamilyAssigner import FamilyAssignerNode
from components.importexport.las import import_to_db
from components.petrophysics.project_statistics import ProjectStatisticsNode
from settings import BASE_DIR
from tasks import async_get_basic_log_stats, async_log_resolution

PATH_TO_TEST_DATA = os.path.join(BASE_DIR, 'test_data', 'ProjectData')


class TestProjectStatisticsNode(TestCase):
    def setUp(self) -> None:
        _s = RedisStorage()
        _s.flush_db()

        for f in os.listdir(PATH_TO_TEST_DATA):
            if f.startswith('100_LAS') or \
               f.startswith('101_LAS') or \
               f.startswith('200_LAS'):
                import_to_db(filename=os.path.join(PATH_TO_TEST_DATA, f))

        p = Project()
        for well_name in p.list_wells():
            async_get_basic_log_stats(well_name)
            async_log_resolution(well_name)

        FamilyAssignerNode.run()


    def test_node_works(self):
        print('run')
        node = ProjectStatisticsNode()
        node.run()
