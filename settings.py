import os

BASE_DIR = os.path.dirname(__file__)

# REDIS STORAGE CONFIG
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
REDIS_HOST = os.environ.get('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_DB', 1)

