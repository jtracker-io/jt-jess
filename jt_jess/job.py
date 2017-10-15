import uuid
import json
import etcd3
import networkx as nx

from .queue import get_queues
from .jt_services import get_owner_id_by_name
from .jt_services import get_job_execution_plan

from .config import JESS_ETCD_HOST
from .config import JESS_ETCD_PORT
from .config import JESS_ETCD_ROOT


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=JESS_ETCD_HOST, port=JESS_ETCD_PORT,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None)


def get_jobs_by_executor(owner_name, queue_id, executor_id, state=None):
    # relevant record in ETCD store
    # /jt:jess/job_queue.id:1922f389-0673-4b71-ae7e-2ca0f86c6d0e/owner.id
    # /jt:jess/job_queue.id:1922f389-0673-4b71-ae7e-2ca0f86c6d0e/executor@executors/id:d4b319ad-08df-4c0b-9a31-e061e97a7b93
    # /jt:jess/executor.id:d4b319ad-08df-4c0b-9a31-e061e97a7b93/job@running_jobs/id:0d912b9d-565e-4330-8c36-7b727c8d10a4

    owner_id = get_owner_id_by_name(owner_name)
    jobs = []

    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                       'job_queue.id:%s' % queue_id, 'owner.id']))

        if r0 and r0[0] and owner_id != r0[0].decode(
                "utf-8"):  # specified job queue does not belong to the specified owner
            return

        executor = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                             'job_queue.id:%s' % queue_id,
                                             'executor@executors/id:%s' % executor_id]))

        if not executor:
            return  # incorrect executor_id

        jobs_prefix = '/'.join([JESS_ETCD_ROOT,
                                'executor.id:%s' % executor_id,
                                'job@%s' % ('' if state is None else '%s_jobs/id:' % state)])

        r = etcd_client.get_prefix(key_prefix=jobs_prefix, sort_target='CREATE', sort_order='descend')

        for value, meta in r:
            job_state, job_id = meta.key.decode('utf-8').split('/')[-2:]  # the last one is job ID
            jobs.append(dict(id=job_id.replace('id:', ''), state=job_state.split('@')[-1].replace('_jobs', '')))

    return jobs


def get_jobs(owner_name, queue_id, job_id=None, state=None):
    # TODO: have to make it very efficient to find job by ID, can't query all jobs from ETCD then filter the hits
    #       maybe we can search one job state at a time to get the job with supplied id
    # Job ETCD key eg: /jt:jess/job_queue.id:{queue_id}/job@jobs/state:queued/id:{job_id}
    # Using generator will definitely help avoid retrieving all jobs at once, instead we can batch by state first
    # then by job UUID first two characters, eg, 00, 01, 02 ...
    try:
        owner_id = get_owner_id_by_name(owner_name)
    except Exception as err:
        raise Exception(str(err))

    jobs = []

    if owner_id:
        r0 = etcd_client.get('/'.join([JESS_ETCD_ROOT,
                                       'job_queue.id:%s' % queue_id, 'owner.id']))

        if r0 and r0[0] and owner_id != r0[0].decode(
                "utf-8"):  # specified job queue does not belong to the specified owner
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

            if job_id is not None and job_id != job.get('id'):  # if job_id specified
                continue

            tasks_prefix = '/'.join([JESS_ETCD_ROOT,
                                     'job_queue.id:%s' % queue_id,
                                     'job.id:%s' % job['id'],
                                     'task@tasks/name:'])

            r1 = etcd_client.get_prefix(key_prefix=tasks_prefix)

            tasks = dict()
            G = nx.DiGraph()
            root = {''}
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
                # print(current_task)
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

    if queues:  # should only have one queue with the specified ID
        workflow_owner = queues[0].get('workflow_owner.name')
        workflow_name = queues[0].get('workflow.name')
        workflow_version = queues[0].get('workflow.ver')

        # later we may enable the support that job name must be globally unique
        # TODO: validate Job JSON

        job_with_execution_plan = get_job_execution_plan(workflow_owner, workflow_name, workflow_version, job_json)

        if job_with_execution_plan:
            job_json['id'] = str(uuid.uuid4())
            job_name = job_json['name'] if job_json.get('name') else '_unnamed'
            etcd_client.put('%s/job_queue.id:%s/job@jobs/state:queued/id:%s/name:%s/job_file' %
                            (JESS_ETCD_ROOT, queue_id, job_json['id'], job_name), value=json.dumps(job_json))
            for task in job_with_execution_plan.pop('tasks'):
                etcd_client.put('%s/job_queue.id:%s/job.id:%s/task@tasks/name:%s/state:queued/task_file' %
                                (JESS_ETCD_ROOT, queue_id, job_json['id'], task['task']), value=json.dumps(task))

        return {'enqueued': True, 'job.id': job_json.get('id')}
    else:
        raise Exception('Job enqueue failed, please make sure parameters provided are correct')


def update_job_state(owner_name, queue_id, executor_id, job_id):
    # get state for every task in the job
    job = get_jobs(owner_name, queue_id, job_id=job_id, state='running')
    if not job:
        return  # something horribly wrong, do nothing for now
    else:
        job = job[0]

    # print(json.dumps(job))

    # if any task is in running state, do nothing and return
    if job.get('tasks_by_state').get('running', []):
        return

    # Example old key:
    # /jt:jess
    # /job_queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9
    # /job@jobs
    # /state:queued
    # /id:240b9fe6-df94-49f5-8364-6c58a1d4a9cb
    # /name:_unnamed
    # /job_file
    job_etcd_key_old = '/'.join([
        JESS_ETCD_ROOT,
        'job_queue.id:%s' % queue_id,
        'job@jobs',
        'state:running',
        'id:%s' % job_id,
        'name:%s' % job.get('name'),
        'job_file'
    ])

    job_r = etcd_client.get(job_etcd_key_old)
    job_etcd_value_old = job_r[0].decode("utf-8")

    # Example old key
    # /jt:jess
    # /executor.id:f3a00ff7-0685-460f-a0f3-821afae93625
    # /job@running_jobs
    # /id:107b1343-591a-4f4a-b867-95cf83d2043d
    exec_job_etcd_key_old = '/'.join([
        JESS_ETCD_ROOT,
        'executor.id:%s' % executor_id,
        'job@running_jobs',
        'id:%s' % job_id
    ])

    # if all tasks are in completed state, the job is completed too
    # update job state of the job itself and the one under any executor that ran any task of the job
    if len(job.get('tasks_by_state').get('completed', [])) == len(job.get('tasks_by_name')):
        job_etcd_key_new = job_etcd_key_old.replace('state:running', 'state:completed')
        job_etcd_value_new = job_etcd_value_old
        exec_job_etcd_key_new = exec_job_etcd_key_old.replace('job@running_jobs', 'job@completed_jobs')

        etcd_client.transaction(
            compare=[
                etcd_client.transactions.version(job_etcd_key_old) > 0,
                etcd_client.transactions.version(exec_job_etcd_key_old) > 0,
            ],
            success=[
                etcd_client.transactions.delete(job_etcd_key_old),
                etcd_client.transactions.put(job_etcd_key_new, job_etcd_value_new),
                etcd_client.transactions.delete(exec_job_etcd_key_old),
                etcd_client.transactions.put(exec_job_etcd_key_new, ''),
            ],
            failure=[]
        )

        return 'Job Completed'

    # if any task is in failed state, the job is failed too
    # update job state of the job itself and the one under any executor that ran any task of the job
    if job.get('tasks_by_state').get('failed', []):
        job_etcd_key_new = job_etcd_key_old.replace('state:running', 'state:failed')
        job_etcd_value_new = job_etcd_value_old
        exec_job_etcd_key_new = exec_job_etcd_key_old.replace('job@running_jobs', 'job@failed_jobs')

        etcd_client.transaction(
            compare=[
                etcd_client.transactions.version(job_etcd_key_old) > 0,
                etcd_client.transactions.version(exec_job_etcd_key_old) > 0,
            ],
            success=[
                etcd_client.transactions.delete(job_etcd_key_old),
                etcd_client.transactions.put(job_etcd_key_new, job_etcd_value_new),
                etcd_client.transactions.delete(exec_job_etcd_key_old),
                etcd_client.transactions.put(exec_job_etcd_key_new, ''),
            ],
            failure=[]
        )

        return 'Job Failed'


# needs to revisit this function later, it seems not the best solution
# one case to consider is what if there are tasks belonging to this job are still running
def stop_job(owner_name=None, action_type=None, queue_id=None,
                         job_id=None, executor_id=None, user_id=None):

    if action_type not in ('cancel', 'suspend'):
        return "Unrecognized action"

    # get the job first
    job = None
    if executor_id and not user_id:  # requested from an executor to cancel a job
        job = get_jobs(owner_name, queue_id, job_id=job_id, state='running')
        if job:
            job = job[0]
        else:
            return 'Specified job is not running'  # do nothing for now
    elif not executor_id and user_id:
        return 'Not implemented yet'
    else:
        return 'Must specify either executor_id or user_id'

    # print(json.dumps(job))

    # Example old key:
    # /jt:jess
    # /job_queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9
    # /job@jobs
    # /state:queued
    # /id:240b9fe6-df94-49f5-8364-6c58a1d4a9cb
    # /name:_unnamed
    # /job_file
    job_etcd_key_old = '/'.join([
        JESS_ETCD_ROOT,
        'job_queue.id:%s' % queue_id,
        'job@jobs',
        'state:running',
        'id:%s' % job_id,
        'name:%s' % job.get('name'),
        'job_file'
    ])

    job_r = etcd_client.get(job_etcd_key_old)
    job_etcd_value_old = job_r[0].decode("utf-8")

    # Example old key
    # /jt:jess
    # /executor.id:f3a00ff7-0685-460f-a0f3-821afae93625
    # /job@running_jobs
    # /id:107b1343-591a-4f4a-b867-95cf83d2043d
    exec_job_etcd_key_old = '/'.join([
        JESS_ETCD_ROOT,
        'executor.id:%s' % executor_id,
        'job@running_jobs',
        'id:%s' % job_id
    ])

    if action_type == 'cancel':
        new_state = 'cancelled'
    elif action_type == 'suspend':
        new_state = 'suspended'

    job_etcd_key_new = job_etcd_key_old.replace('state:running', 'state:%s' % new_state)
    job_etcd_value_new = job_etcd_value_old
    exec_job_etcd_key_new = exec_job_etcd_key_old.replace('job@running_jobs', 'job@%s_jobs' % new_state)

    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(job_etcd_key_old) > 0,
            etcd_client.transactions.version(exec_job_etcd_key_old) > 0,
        ],
        success=[
            etcd_client.transactions.delete(job_etcd_key_old),
            etcd_client.transactions.put(job_etcd_key_new, job_etcd_value_new),
            etcd_client.transactions.delete(exec_job_etcd_key_old),
            etcd_client.transactions.put(exec_job_etcd_key_new, ''),
        ],
        failure=[]
    )

    return 'Job %s' % new_state
