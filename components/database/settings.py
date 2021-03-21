import os

STORAGE_USER = os.environ.get('STORAGE_USER', 'postgres')
STORAGE_PASSWORD = os.environ.get('STORAGE_PASSWORD', 'postgres')
STORAGE_HOST = os.environ.get('STORAGE_HOST', 'localhost')
STORAGE_PORT = os.environ.get('STORAGE_PORT', 5432)
STORAGE_DB = os.environ.get('STORAGE_DB', 'test_db')

REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = os.environ.get('REDIS_PORT', 6379)
REDIS_DB = os.environ.get('REDIS_DB', 1)

META_REFERENCE = -10000
MISSING_VALUE = -999.25
