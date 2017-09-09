import etcd3
import uuid
import requests
import json
import networkx as nx
from .exceptions import OwnerNameNotFound, AMSNotAvailable, WorklowNotFound, WRSNotAvailable

# settings, need to move out to config

AMS_URL = 'http://localhost:12012/api/jt-ams/v0.1'
WRS_URL = 'http://localhost:12015/api/jt-wrs/v0.1'
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
    request_url = '%s/workflows/id/%s' % (WRS_URL.strip('/'), workflow_id)
    if workflow_version:
        request_url += '/ver/%s' % workflow_version
    try:
        r = requests.get(request_url)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        raise WorklowNotFound(workflow_id)

    return json.loads(r.text)


#call JT-WRS REST endpoint: /workflows/owner/{owner_name}/workflow/{workflow_name}/ver/{workflow_version}/job_execution_plan
def _get_job_execution_plan(owner_name, workflow_name, workflow_verion, job_json):
    request_url = '%s/workflows/owner/%s/workflow/%s/ver/%s/job_execution_plan' % (WRS_URL.strip('/'),
                                                                 owner_name, workflow_name, workflow_verion)
    try:
        r = requests.put(request_url, json=job_json)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        raise WorklowNotFound("%s.%s/%s" % (owner_name, workflow_name, workflow_verion))

    return json.loads(r.text)


def get_queues(owner_name, workflow_name=None, workflow_version=None, workflow_owner_name=None, queue_id=None):
    owner_id = _get_owner_id_by_name(owner_name)
    queues = []

    if owner_id:
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

            #print("k:%s, v:%s" % (k, v))

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
                workflow = _get_workflow_by_id(queue.get('workflow.id'), workflow_version)
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
    else:
        raise OwnerNameNotFound(Exception("Specific owner name not found: %s" % owner_name))


def get_jobs(owner_name, queue_id, job_id=None, state=None):
    owner_id = _get_owner_id_by_name(owner_name)
    jobs = []

    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % queue_id, 'owner.id']))

        if r0 and r0[0] and owner_id != r0[0].decode("utf-8"):  # specified job queue does not belong to the specified owner
            return

        jobs_prefix = '/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % queue_id,
                                            'job@jobs/state:%s' % ('' if state is None else state + '/')])

        r = etcd_client.get_prefix(key_prefix=jobs_prefix, sort_target='CREATE', sort_order='descend')

        for value, meta in r:
            k = meta.key.decode('utf-8').replace('/'.join([JESS_ETCD_ROOT,
                                            'job_queue.id:%s' % queue_id,
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

            print(job.get('id'))
            if job_id is not None and job_id != job.get('id'):  # if job_id specified
                continue

            tasks_prefix = '/'.join([JESS_ETCD_ROOT,
                                    'job_queue.id:%s' % queue_id,
                                    'job.id:%s' % job['id'],
                                    'task@tasks/name:'])

            r1 = etcd_client.get_prefix(key_prefix=tasks_prefix)

            tasks = dict()
            G = nx.DiGraph()
            root = set([''])
            for value, meta in r1:
                k = meta.key.decode('utf-8').replace('/'.join([JESS_ETCD_ROOT,
                                                               'job_queue.id:%s' % queue_id,
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

                task_name = task.get('name')
                dependent_tasks = json.loads(task.get('task_file')).get('depends_on')
                if dependent_tasks:
                    for dt in dependent_tasks:
                        if not dt.startswith('completed@'):
                            continue
                        dt = dt.split('@')[1]
                        G.add_edge(dt, task_name)
                else:
                    G.add_edge('', task_name)  # this step has no dependency, use '' as root node to be parent task

                tasks[task_name] = task

            task_lists = {}
            for current_task in nx.topological_sort(G):  # generate a linear task execution plan
                print(current_task)
                if current_task in ('', 'download'):  # TODO: need to deal with gather step that depends on scatter step
                    continue
                task_state = tasks.get(current_task).get('state')
                if task_state not in task_lists:
                    task_lists[task_state] = []

                task_lists[task_state].append(
                    {current_task: tasks.get(current_task)}
                )

            job['tasks_by_name'] = tasks
            job['tasks_by_state'] = task_lists
            jobs.append(job)

        if jobs:
            return jobs


def enqueue_job(owner_name, queue_id, job_json):
    # get workflow information first
    queues = get_queues(owner_name, queue_id=queue_id)

    if queues: # should only have one queue with the specified ID
        workflow_owner = queues[0].get('workflow_owner.name')
        workflow_name = queues[0].get('workflow.name')
        workflow_version = queues[0].get('workflow.ver')

        # later we may enable the support that job name must be globally unique

        job_with_execution_plan = _get_job_execution_plan(workflow_owner, workflow_name, workflow_version, job_json)

        if job_with_execution_plan:
            job_json['id'] = str(uuid.uuid4())
            job_name = job_json['name'] if job_json.get('name') else '_unnamed'
            etcd_client.put('%s/job_queue.id:%s/job@jobs/state:queued/id:%s/name:%s/job_file' %
                            (JESS_ETCD_ROOT, queue_id, job_json['id'], job_name), value=json.dumps(job_json))
            for task in job_with_execution_plan.pop('tasks'):
                etcd_client.put('%s/job_queue.id:%s/job.id:%s/task@tasks/name:%s/state:queued/task_file' %
                                (JESS_ETCD_ROOT, queue_id, job_json['id'], task['task']), value=json.dumps(task))

        return get_jobs(owner_name, queue_id, job_json.get('id'))[0]


def next_task(owner_name, queue_id, executor, job_id):
    # verify executor already registered under the job queue

    # find candidate job(s)
    # let's check running jobs first
    jobs = get_jobs(owner_name, queue_id, job_id, 'running')
    if jobs:
        for job in jobs:
            # TODO: put this in a function that can be called from different places
            for task in job.get('tasks_by_state', {}).get('queued', []):
                task_to_be_scheduled = list(task.values())[0]
                task_to_be_scheduled['job.id'] = job.get('id')

                # using transaction to update job state and task state, return task only when it's success
                # 1) if job key exists: /jthub:jes/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job@jobs/state:queued/id:d66b3f18-834a-4129-9d4c-9af975afee44/name:first_job/job_file
                # 2) if task key exists: /jthub:jes/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job.id:d66b3f18-834a-4129-9d4c-9af975afee44/task@tasks/name:prepare_metadata_xml/state:queued/task_file
                # 2.5) look for depends_on tasks (if any), and copy over their output to current task's input
                # 3) then create new job key and task key, and delete old ones
                # if precondition fails, return None, ie, no task returned
                job_etcd_key = '/'.join([
                    JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'job@jobs',
                    'state:running',
                    'id:%s' % job.get('id'),
                    'name:%s' % job.get('name'),
                    'job_file'
                ])
                task_etcd_key = '/'.join([
                    JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'job.id:%s' % job.get('id'),
                    'task@tasks',
                    'name:%s' % task_to_be_scheduled.get('name'),
                    'state:queued',
                    'task_file'
                ])
                task_r = etcd_client.get(task_etcd_key)
                task_file = task_r[0].decode("utf-8")

                # TODO: modify task_file as needed
                # check dependent tasks to see whether they are completed
                dependency_ready = True
                depends_on = json.loads(task_file).get('depends_on')
                dtasks = {}
                if depends_on:
                    for dt in depends_on:
                        dt_name = dt.split('@')[1]
                        # TODO: deal with gather dependencies later
                        if dt_name == 'download': continue

                        if job.get('tasks_by_name').get(dt_name).get('state') != 'completed':
                            dependency_ready = False
                            break
                        dtasks[dt_name] = dt

                if not dependency_ready:
                    continue

                # TODO: check current task for parameters depending on parent tasks, fetch output from parent tasks as needed


                new_task_etcd_key = task_etcd_key.replace('/state:queued/', '/state:running/')

                etcd_client.transaction(
                    compare=[
                        etcd_client.transactions.version(job_etcd_key) > 0,  # test key exists
                        etcd_client.transactions.version(task_etcd_key) > 0,  # test key exists
                    ],
                    success=[
                        etcd_client.transactions.put(new_task_etcd_key, task_file),
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )

                # TODO: add executor information in task_file

                task_to_be_scheduled['state'] = 'running'
                return task_to_be_scheduled

    # if no task ready in running jobs, try find in queued jobs
    jobs = get_jobs(owner_name, queue_id, job_id, 'queued')
    if jobs:
        for job in jobs:
            for task in job.get('tasks_by_state', {}).get('queued', []):
                task_to_be_scheduled = list(task.values())[0]
                task_to_be_scheduled['job.id'] = job.get('id')

                # using transaction to update job state and task state, return task only when it's success
                # 1) if job key exists: /jthub:jes/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job@jobs/state:queued/id:d66b3f18-834a-4129-9d4c-9af975afee44/name:first_job/job_file
                # 2) if task key exists: /jthub:jes/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job.id:d66b3f18-834a-4129-9d4c-9af975afee44/task@tasks/name:prepare_metadata_xml/state:queued/task_file
                # 2.5) look for depends_on tasks (if any), and copy over their output to current task's input
                # 3) then create new job key and task key, and delete old ones
                # if precondition fails, return None, ie, no task returned
                job_etcd_key = '/'.join([
                    JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'job@jobs',
                    'state:queued',
                    'id:%s' % job.get('id'),
                    'name:%s' % job.get('name'),
                    'job_file'
                ])
                task_etcd_key = '/'.join([
                    JESS_ETCD_ROOT,
                    'job_queue.id:%s' % queue_id,
                    'job.id:%s' % job.get('id'),
                    'task@tasks',
                    'name:%s' % task_to_be_scheduled.get('name'),
                    'state:queued',
                    'task_file'
                ])

                job_r = etcd_client.get(job_etcd_key)
                task_r = etcd_client.get(task_etcd_key)
                job_file = job_r[0].decode("utf-8")
                task_file = task_r[0].decode("utf-8")

                # TODO: modify task_file as needed
                # check dependent tasks to see whether they are completed
                dependency_ready = True
                depends_on = json.loads(task_file).get('depends_on')
                if depends_on:
                    for dt in depends_on:
                        dt_name = dt.split('@')[1]
                        if job.get('tasks_by_name').get(dt_name).get('state') != 'completed':
                            dependency_ready = False
                            break

                if not dependency_ready:
                    continue

                new_job_etcd_key = job_etcd_key.replace('/state:queued/', '/state:running/')
                new_task_etcd_key = task_etcd_key.replace('/state:queued/', '/state:running/')

                etcd_client.transaction(
                    compare=[
                        etcd_client.transactions.version(job_etcd_key) > 0,
                        etcd_client.transactions.version(task_etcd_key) > 0,
                    ],
                    success=[
                        etcd_client.transactions.put(new_job_etcd_key, job_file),
                        etcd_client.transactions.delete(job_etcd_key),
                        etcd_client.transactions.put(new_task_etcd_key, task_file),
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )

                task_to_be_scheduled['state'] = 'running'
                # TODO: add executor information in task_file

                return task_to_be_scheduled

def complete_task(owner_name, queue_id, job_id, task_name, result):
    jobs = get_jobs(owner_name, queue_id, job_id, 'running')
    #print(jobs)
    if not jobs:
        # raise JobNotFound error here
        return

    job = jobs[0]
    task_etcd_key = '/'.join([
        JESS_ETCD_ROOT,
        'job_queue.id:%s' % queue_id,
        'job.id:%s' % job.get('id'),
        'task@tasks',
        'name:%s' % task_name,
        'state:running',
        'task_file'
    ])

    task = job.get('tasks_by_name').get(task_name)

    task_r = etcd_client.get(task_etcd_key)
    try:
        task_file = task_r[0].decode("utf-8")
    except:
        # raise TaskNotFound or TaskNotInRunningState
        return

    #print(task)
    #print(task_file)

    # TODO: verify executor is the same as expected

    # TODO: update task_file with result reported by executor

    # write the updated task_file back
    new_task_etcd_key = task_etcd_key.replace('/state:running/', '/state:completed/')

    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(task_etcd_key) > 0,
        ],
        success=[
            etcd_client.transactions.put(new_task_etcd_key, task_file),
            etcd_client.transactions.delete(task_etcd_key),
        ],
        failure=[]
    )

    task['state'] = 'completed'
    return task
