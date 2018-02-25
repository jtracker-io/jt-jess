import uuid
import json
import etcd3
from .jt_services import get_owner_id_by_name

from .config import ETCD_HOST
from .config import ETCD_PORT
from .config import JESS_ETCD_ROOT


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None)


def register_executor(owner_name, queue_id, node_id):
    owner_id = get_owner_id_by_name(owner_name)
    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                       'job_queue.id:%s' % queue_id, 'owner.id']))
        if r0 and r0[0] and owner_id != r0[0].decode(
                "utf-8"):  # specified job queue does not belong to the specified owner
            return
    else:
        raise Exception('Specified owner name does not exist')

    key = '/'.join([JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'node.id:%s' % node_id,
                    'executor@executors/id:'])

    rv = etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(key) > 0,  # test key exists
        ],
        success=[],  # do nothing
        failure=[
            etcd_client.transactions.put(key, str(uuid.uuid4()))  # assign new UUID for new executor
        ]
    )

    r0 = etcd_client.get(key)
    if r0 and r0[0]:
        return {'id': r0[0].decode('utf-8')}
    else:
        raise Exception('Registering executor failed')


def get_executors(owner_name, queue_id=None, executor_id=None):
    print("not implemented yet")
    return "not implemented yet"
