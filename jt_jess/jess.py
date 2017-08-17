import etcd3
import uuid
import requests
import json
from .exceptions import OwnerNameNotFound, AMSNotAvailable, WorklowNotFound, WRSNotAvailable

# settings, need to move out to config

AMS_URL = 'http://localhost:1206/api/jt-ams/v0.1'
WRS_URL = 'http://localhost:1207/api/jt-wrs/v0.1'
JESS_ETCD_ROOT = '/jthub:jes'

etcd_client = etcd3.client()


def _get_owner_id_by_name(owner_name):
    request_url = '%s/accounts/%s' % (AMS_URL.strip('/'), owner_name)
    try:
        r = requests.get(request_url)
    except:
        raise AMSNotAvailable('AMS service unavailable')

    if r.status_code != 200:
        raise OwnerNameNotFound(owner_name)

    return json.loads(r.text).get('id')


def _get_workflow_by_id(workflow_id, workflow_version=None):
    request_url = '%s/workflows/_id/%s' % (WRS_URL.strip('/'), workflow_id)
    if workflow_version:
        request_url += '/_ver/%s' % workflow_version
    try:
        r = requests.get(request_url)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        raise WorklowNotFound(workflow_id)

    return json.loads(r.text)


#call JT-WRS REST endpoint: /workflows/{owner_name}/{workflow_name}/{workflow_version}/_job_execution_plan
def _get_job_execution_plan(owner_name, workflow_name, workflow_verion, jobjson):
    request_url = '%s/workflows/%s/%s/%s/_job_execution_plan' % (WRS_URL.strip('/'),
                                                                 owner_name, workflow_name, workflow_verion)
    try:
        r = requests.put(request_url, json=jobjson)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        raise WorklowNotFound("%s.%s/%s" % (owner_name, workflow_name, workflow_verion))

    return json.loads(r.text)


def get_job_queues(owner_name, workflow_name=None, workflow_version=None, workflow_owner_name=None, job_queue_id=None):
    owner_id = _get_owner_id_by_name(owner_name)
    job_queues = []

    if owner_id:
        # find the workflows' name and id first
        job_queues_prefix = '/'.join([JESS_ETCD_ROOT,
                                            'owner.id:%s' % owner_id,
                                            'workflow.id:'])

        r = etcd_client.get_prefix(key_prefix=job_queues_prefix)

        for value, meta in r:
            k = meta.key.decode('utf-8').replace(JESS_ETCD_ROOT + '/', '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value, deal with it later

            #print("k:%s, v:%s" % (k, v))

            if not k.endswith('/id'):
                continue

            if job_queue_id and v != job_queue_id:  # get only specified job queue
                continue

            job_queue = {
                'id': v,
                'owner.name': owner_name
            }

            for new_k_vs in k.split('/'):
                if new_k_vs == 'job_queue@job_queues':
                    continue
                if ':' in new_k_vs:
                    new_k, new_v = new_k_vs.split(':', 1)
                    job_queue[new_k] = new_v

            try:
                workflow = _get_workflow_by_id(job_queue.get('workflow.id'), workflow_version)
            except WorklowNotFound:
                continue
            except WRSNotAvailable:
                raise WRSNotAvailable('WRS service temporarily unavailable')

            if not workflow or (workflow_owner_name is not None and workflow_owner_name != workflow.get('owner.name')) \
                    or (workflow_name and workflow_name != workflow.get('name')):
                continue

            job_queue['workflow.name'] = workflow.get('name')
            job_queue['workflow_owner.id'] = workflow.get('owner.id')
            job_queue['workflow_owner.name'] = workflow.get('owner.name')

            job_queues.append(job_queue)

        return job_queues
    else:
        raise OwnerNameNotFound(Exception("Specific owner name not found: %s" % owner_name))


def get_jobs(owner_name, job_queue_id, job_id=None, state=None):
    owner_id = _get_owner_id_by_name(owner_name)
    jobs = []

    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % job_queue_id, 'owner.id']))

        if r0 and r0[0] and owner_id != r0[0].decode("utf-8"):  # specified job queue does not belong to the specified owner
            return

        jobs_prefix = '/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % job_queue_id,
                                            'job@jobs/id:'])

        r = etcd_client.get_prefix(key_prefix=jobs_prefix)

        for value, meta in r:
            k = meta.key.decode('utf-8').replace('/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % job_queue_id,
                                            'job@jobs/']), '', 1)
            try:
                v = value.decode("utf-8")
            except:
                v = None  # assume binary value, deal with it later

            job = {}

            for new_k_vs in k.split('/'):
                if new_k_vs == 'job@jobs':
                    continue
                if ':' in new_k_vs:
                    new_k, new_v = new_k_vs.split(':', 1)
                    job[new_k] = new_v
                else:
                    job[new_k_vs] = v

            if job_id is not None and job_id != job.get('id'):  # if job_id specified
                continue

            print(state)
            if state is not None and state != job.get('state'):
                continue

            tasks_prefix = '/'.join([JESS_ETCD_ROOT,
                                    'job_queue.id:%s' % job_queue_id,
                                    'job.id:%s' % job['id'],
                                    'task@tasks/name:'])

            r1 = etcd_client.get_prefix(key_prefix=tasks_prefix)

            tasks = []
            for value, meta in r1:
                k = meta.key.decode('utf-8').replace('/'.join([JESS_ETCD_ROOT,
                                                               'job_queue.id:%s' % job_queue_id,
                                                               'job.id:%s/task@tasks/' % job['id']]), '', 1)
                try:
                    v = value.decode("utf-8")
                except:
                    v = None  # assume binary value, deal with it later

                task = {}

                for new_k_vs in k.split('/'):
                    if new_k_vs == 'task@tasks':
                        continue
                    if ':' in new_k_vs:
                        new_k, new_v = new_k_vs.split(':', 1)
                        task[new_k] = new_v
                    else:
                        task[new_k_vs] = v

                tasks.append(task)

            job['tasks'] = tasks
            jobs.append(job)

        if jobs:
            return jobs


def enqueue_job(owner_name, job_queue_id, jobjson):
    # get workflow information first
    job_queues = get_job_queues(owner_name, job_queue_id=job_queue_id)

    if job_queues: # should only have one queue with the specified ID
        workflow_owner = job_queues[0].get('workflow_owner.name')
        workflow_name = job_queues[0].get('workflow.name')
        workflow_version = job_queues[0].get('workflow.ver')

        # later we may enable the support that job name must be globally unique

        job_with_execution_plan = _get_job_execution_plan(workflow_owner, workflow_name, workflow_version, jobjson)

        if job_with_execution_plan:
            jobjson['id'] = str(uuid.uuid4())
            job_name = jobjson['name'] if jobjson['name'] else '_unnamed'
            etcd_client.put('%s/job_queue.id:%s/job@jobs/id:%s/name:%s/state:queued/job_file' %
                            (JESS_ETCD_ROOT, job_queue_id, jobjson['id'], job_name), value=json.dumps(jobjson))
            for task in job_with_execution_plan.pop('tasks'):
                etcd_client.put('%s/job_queue.id:%s/job.id:%s/task@tasks/name:%s/state:queued/task_file' %
                                (JESS_ETCD_ROOT, job_queue_id, jobjson['id'], task['task']), value=json.dumps(task))

        return get_jobs(owner_name, job_queue_id, jobjson.get('id'))[0]


def update_owner():
    pass


def delete_owner():
    pass


def add_member():
    pass


def delete_member():
    pass
