import etcd3
import uuid
import requests
import json
from .jtracker import JTracker
from .exceptions import AccountNameNotFound, AMSNotAvailable

# settings, need to move out to config

AMS_URL = 'http://localhost:1206/api/jt-ams/v0.1'
WRS_ETCD_ROOT = '/jthub:jes'

etcd_client = etcd3.client()


def _get_account_id_by_name(account_name):
    request_url = '%s/accounts/%s' % (AMS_URL.strip('/'), account_name)
    try:
        r = requests.get(request_url)
    except:
        raise AMSNotAvailable('AMS service unavailable')

    if r.status_code != 200:
        raise AccountNameNotFound(account_name)

    return json.loads(r.text).get('id')


def get_job_queues(account_name, workflow_name=None, workflow_version=None, workflow_owner=None):
    account_id = _get_account_id_by_name(account_name)
    job_queues = []

    if account_id:

        # /jthub:jes/account.id:1097accf-601c-4f9f-88b0-031ec231f9e2/workflow.id:

        # find the workflows' name and id first
        job_queues_prefix = '/'.join([WRS_ETCD_ROOT,
                                            'account.id:%s' % account_id,
                                            'workflow.id:'])

        print(job_queues_prefix)
        r = etcd_client.get_prefix(key_prefix=job_queues_prefix)

        for value, meta in r:
            k = meta.key.decode('utf-8').replace(WRS_ETCD_ROOT + '/', '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value, deal with it later

            print("k:%s, v:%s" % (k, v))

            job_queue = dict()

            for new_k_vs in k.split('/'):
                if ':' in new_k_vs:
                    new_k, new_v = new_k_vs.split(':', 1)
                else:
                    new_k = new_k_vs
                    new_v = v

                job_queue[new_k] = new_v

            job_queues.append(job_queue)

        return job_queues
    else:
        raise AccountNameNotFound(Exception("Specific account name not found: %s" % account_name))


def register_workflow(account_name, account_type):
    id = str(uuid.uuid4())

    key = '/'.join([AMS_ROOT, ACCOUNT_PATH, '%s:%s' % ('name', account_name)])
    r = etcd_client.put(key, id)

    key_prefix = '/'.join([AMS_ROOT, ACCOUNT_PATH, 'data', '%s:%s' % ('id', id)])
    r = etcd_client.put('%s/name' % key_prefix, account_name)

    if account_type == 'org':
        r = etcd_client.put('%s/is_org' % key_prefix, '1')
    else:
        r = etcd_client.put('%s/is_org' % key_prefix, '')

    return get_account(account_name)


def update_account():
    pass


def delete_account():
    pass


def add_member():
    pass


def delete_member():
    pass
