import etcd3
import uuid
import requests
import json
from .exceptions import AccountNameNotFound, AMSNotAvailable

# settings, need to move out to config

AMS_URL = 'http://localhost:1206/api/jt-ams/v0.1'
WRS_ETCD_ROOT = '/jthub:wrs'

etcd_client = etcd3.client()


def _get_account_id_by_name(account_name):
    request_url = '%s/accounts/%s' % (AMS_URL.strip('/'), account_name)
    try:
        r = requests.get(request_url)
    except:
        raise AMSNotAvailable('AMS service unavailable')

    if r.status_code != 200:
        raise AccountNameNotFound(account_name)

    return json.loads(r.text).get('_id')


def get_workflows(account_name, workflow_name=None, workflow_version=None):
    account_id = _get_account_id_by_name(account_name)

    if account_id:
        workflows = []

        # find the workflows' name and id first
        workflow_name_id_prefix = '/'.join([WRS_ETCD_ROOT,
                                            'account._id:%s' % account_id,
                                            'workflow/'])

        if workflow_name:
            key_search_prefix = '%s_name:%s/_id' % (workflow_name_id_prefix, workflow_name)
        else:
            key_search_prefix = '%s_name' % workflow_name_id_prefix

        r = etcd_client.get_prefix(key_prefix=key_search_prefix, sort_target='KEY')

        for value, meta in r:
            k = meta.key.decode('utf-8').replace(workflow_name_id_prefix, '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value, deal with it later

            print("k:%s, v:%s" % (k, v))

            workflow_name = k.strip('_name:').rstrip('/_id')
            workflow_id = v

            workflow = {
                "_id": workflow_id,
                "_name": workflow_name
            }

            # ideally we don't read values yet, but python-etcd does not have this option
            workflow_prefix = '/'.join([WRS_ETCD_ROOT, 'workflow', '_id:%s/' % workflow_id])

            r2 = etcd_client.get_prefix(key_prefix=workflow_prefix, sort_target='KEY')

            for value2, meta2 in r2:
                k = meta2.key.decode('utf-8').replace(workflow_prefix, '', 1)
                try:
                    v = value2.decode("utf-8")
                except:
                    v = None  # assume binary value, deal with it later
                print("k:%s, v:%s" % (k, v))
                parts = k.split('/')

                if ':' in parts[-1]:
                    new_key, new_value = parts[-1].split(':', 1)
                else:
                    new_key, new_value = parts[-1], v

                if isinstance(new_value, str):
                    new_value = new_value.replace('+', '/')

                if new_key.startswith('is_'):
                    new_value = True if new_value and new_value != '0' else False

                if len(parts) == 1:
                    if '@' not in new_key:
                        workflow[new_key] = new_value
                    else:
                        sub_key, sub_type = new_key.split('@', 1)
                        if not sub_type in workflow: workflow[sub_type] = []
                        workflow[sub_type].append({sub_key: new_value})

                elif len(parts) == 2:
                    ver_tag, ver = parts[0].split(':', 1)
                    if workflow_version and not workflow_version == ver:
                        continue

                    ver = 'ver:%s' % ver
                    if ver_tag == 'ver' and not workflow.get(ver):
                        workflow[ver] = {}

                    if '@' not in new_key:
                        workflow[ver][new_key] = new_value
                    else:
                        sub_key, sub_type = new_key.split('@', 1)
                        if sub_type not in workflow[ver]: workflow[ver][sub_type] = []
                        workflow[ver][sub_type].append({sub_key: new_value})

            workflows.append(workflow)

        return workflows
    else:
        raise AccountNameNotFound(Exception("Specific account name not found: %s" % account_name))


def get_workflow(account_name, workflow_name, workflow_version=None):
    workflow = get_workflows(account_name, workflow_name, workflow_version)
    if workflow_version and 'ver:%s' % workflow_version not in workflow[0]:
        return
    elif workflow:
        return workflow[0]


def register_workflow(account_name, account_type):
    _id = str(uuid.uuid4())

    key = '/'.join([AMS_ROOT, ACCOUNT_PATH, '%s:%s' % ('_name', account_name)])
    r = etcd_client.put(key, _id)

    key_prefix = '/'.join([AMS_ROOT, ACCOUNT_PATH, 'data', '%s:%s' % ('_id', _id)])
    r = etcd_client.put('%s/_name' % key_prefix, account_name)

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
