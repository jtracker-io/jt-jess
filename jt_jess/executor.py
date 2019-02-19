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


def register_executor(owner_name, queue_id, node_id, node_info=None):
    if node_info is None:
        node_ip = ''
    else:
        node_ip = node_info.get('node_ip', '')

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
            # assign new UUID for new executor
            etcd_client.transactions.put(key, '%s/node_ip:%s' % (str(uuid.uuid4()), node_ip))
        ]
    )

    r0 = etcd_client.get(key)
    if r0 and r0[0]:
        items = r0[0].decode('utf-8').split('/')
        executor = {'id': items[0]}
        for item in items[1:]:
            k, v = item.split(':')
            executor[k] = v

        return executor
    else:
        raise Exception('Registering executor failed')


def get_executors(owner_name, queue_id=None, node_id=None):
    owner_id = get_owner_id_by_name(owner_name)
    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                       'job_queue.id:%s' % queue_id, 'owner.id']))
        if r0 and r0[0] and owner_id != r0[0].decode(
                "utf-8"):  # specified job queue does not belong to the specified owner
            return
    else:
        raise Exception('Specified owner name does not exist')

    key_prefix = '/'.join([JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'node.id:%s' % ('%s/executor@executors/id:' % node_id if node_id else '')
                    ])

    executors = []
    rv = etcd_client.get_prefix(key_prefix=key_prefix)
    for value, meta in rv:
        k = meta.key.decode('utf-8').replace(JESS_ETCD_ROOT, '', 1)
        k = k + value.decode("utf-8")  # join key and value with a ':'

        executor = {}
        for token in k.split('/'):
            if ':' not in token:
                continue
            k1, v1 = token.split(':')
            if k1.endswith('.id'):
                k1 = k1.replace('.', '_')
            executor[k1] = v1

        executors.append(executor)

    return executors
