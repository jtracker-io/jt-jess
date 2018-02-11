import uuid
import etcd3
from .jt_services import get_owner_id_by_name
from .jt_services import get_workflow_by_name
from .jt_services import get_workflow_by_id
from .exceptions import OwnerNameNotFound
from .exceptions import WorklowNotFound
from .exceptions import QueueCreationFailure
from .exceptions import WRSNotAvailable

from .config import ETCD_HOST
from .config import ETCD_PORT
from .config import JESS_ETCD_ROOT


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None)


def create_queue(owner_name, workflow_name, workflow_version, workflow_owner_name):
    owner_id = get_owner_id_by_name(owner_name)

    if not owner_id:
        raise OwnerNameNotFound(Exception("Specific owner name not found: %s" % owner_name))

    workflow = get_workflow_by_name(workflow_owner_name, workflow_name, workflow_version)

    if not workflow:
        raise WorklowNotFound(Exception("Specific workflow not found: %s.%s/%s" %
                                        workflow_owner_name, workflow_name, workflow_version))

    queue_id = str(uuid.uuid4())

    # save to etcd
    # /jt:jess/owner.id:1097accf-601c-4f9f-88b0-031ec231f9e2/workflow.id:7e7cf044-c7ea-4d84-aafc-eb6ad438ca0e/workflow.ver:0.2.0/job_queue@job_queues/
    queue_etcd_key = '/'.join([JESS_ETCD_ROOT,
                              'owner.id:%s' % owner_id,
                              'workflow.id:%s' % workflow.get('id'),
                              'workflow.ver:%s' % workflow_version,
                              'job_queue@job_queues/id'  # one job queue for each workflow for now, more queues maybe allowed later
                              ])

    # /jt:jess/job_queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/owner.id
    queue_owner_etcd_key = '/'.join([
        JESS_ETCD_ROOT,
        'job_queue.id:%s' % queue_id,
        'owner.id'
    ])

    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(queue_etcd_key) > 0,  # test key exists
        ],
        success=[
        ],
        failure=[
            etcd_client.transactions.put(queue_etcd_key, queue_id),
            etcd_client.transactions.put(queue_owner_etcd_key, owner_id)
        ]
    )

    queues = get_queues(owner_name, queue_id=queue_id)
    if len(queues) != 1:
        raise QueueCreationFailure()

    return queues[0]


def get_queues(owner_name, workflow_name=None, workflow_version=None, workflow_owner_name=None, queue_id=None):
    owner_id = get_owner_id_by_name(owner_name)

    if not owner_id:
        raise OwnerNameNotFound(Exception("Specific owner name not found: %s" % owner_name))

    queues = []
    # find the workflows' name and id first
    queues_prefix = '/'.join([JESS_ETCD_ROOT,
                              'owner.id:%s' % owner_id,
                              'workflow.id:'])

    r = etcd_client.get_prefix(key_prefix=queues_prefix)

    for value, meta in r:
        k = meta.key.decode('utf-8').replace(JESS_ETCD_ROOT + '/', '', 1)
        try:
            v = value.decode("utf-8")
        except:
            v = None  # assume binary value, deal with it later

        # print("k:%s, v:%s" % (k, v))

        if not k.endswith('/id'):
            continue

        if queue_id and v != queue_id:  # get only specified job queue
            continue

        queue = {
            'id': v,
            'owner.name': owner_name
        }

        for new_k_vs in k.split('/'):
            if new_k_vs == 'job_queue@job_queues':
                continue
            if ':' in new_k_vs:
                new_k, new_v = new_k_vs.split(':', 1)
                queue[new_k] = new_v

        try:
            workflow = get_workflow_by_id(queue.get('workflow.id'), workflow_version)
        except WorklowNotFound:
            continue
        except WRSNotAvailable:
            raise WRSNotAvailable('WRS service temporarily unavailable')

        if not workflow or (workflow_owner_name is not None and workflow_owner_name != workflow.get('owner.name')) \
                or (workflow_name and workflow_name != workflow.get('name')):
            continue

        queue['workflow.name'] = workflow.get('name')
        queue['workflow_owner.id'] = workflow.get('owner.id')
        queue['workflow_owner.name'] = workflow.get('owner.name')

        queues.append(queue)

    return queues
