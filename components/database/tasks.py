from celery_conf import app
from components.database.RedisStorage import (build_log_meta_fields_index,
                                              build_well_meta_fields_index,
                                              build_dataset_meta_fields_index)

build_log_meta_fields_index = app.task(build_log_meta_fields_index)
build_dataset_meta_fields_index = app.task(build_dataset_meta_fields_index)
build_well_meta_fields_index = app.task(build_well_meta_fields_index)

