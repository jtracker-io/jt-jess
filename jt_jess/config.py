import os


AMS_URL = os.environ.get('AMS_URI', 'http://localhost:12012/api/jt-ams/v0.1')
WRS_URL = os.environ.get('WRS_URL', 'http://localhost:12015/api/jt-wrs/v0.1')

JESS_ETCD_HOST = 'localhost'
JESS_ETCD_PORT = 2379
JESS_ETCD_ROOT = '/jthub:jes'
