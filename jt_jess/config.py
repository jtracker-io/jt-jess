import os


ams_host = os.environ.get('AMS_HOST', 'localhost')
ams_port = os.environ.get('AMS_PORT', 12012)
AMS_URL = 'http://%s:%s/api/jt-ams/v0.1' % (ams_host, ams_port)

wrs_host = os.environ.get('WRS_HOST', 'localhost')
wrs_port = os.environ.get('WRS_PORT', 12015)
WRS_URL = 'http://%s:%s/api/jt-wrs/v0.1' % (wrs_host, wrs_port)

ETCD_HOST = os.environ.get('ETCD_HOST', 'localhost')
ETCD_PORT = os.environ.get('ETCD_PORT', 2379)

JESS_ETCD_ROOT = '/jt:jess'

JOB_STATES = ('submitted', 'queued', 'running', 'completed', 'failed', 'retry', 'suspended', 'cancelled')
