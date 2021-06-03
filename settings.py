import os

import logging

BASE_DIR = os.path.dirname(__file__)

# REDIS STORAGE CONFIG
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_DB', 1)

# MINIO
MINIO_HOST = os.environ.get('MINIO_HOST', 'localhost')
MINIO_PORT = os.environ.get('MINIO_PORT', 9000)
MINIO_USER = os.environ.get('MINIO_USER', 'gamma')
MINIO_PASSWORD = os.environ.get('MINIO_PASSWORD', 'gamma2021')

# GAMMA CONFIG
DEFAULT_MISSING_VALUE = -999.25
DEFAULT_LQC_NAME = 'LQC'
DEFAULT_MARKERS_NAME = 'Markers'
DEFAULT_DEPTH_UNITS = 'm'

LOGGING_LEVEL = logging.INFO