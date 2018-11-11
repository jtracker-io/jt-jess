import uuid
import json
import etcd3
import networkx as nx

from .queue import get_queues
from .executor import get_executors
from .jt_services import get_owner_id_by_name
from .jt_services import get_job_execution_plan

from .config import ETCD_HOST
from .config import ETCD_PORT
from .config import JESS_ETCD_ROOT
from .config import JOB_STATES


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT,
            ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
            user=None, password=None, grpc_options={
                                                        'grpc.max_send_message_length': 10 * 1024 * 1024,
                                                        'grpc.max_receive_message_length': -1,
                                                    }.items()
            )


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

        r = etcd_client.get_prefix(key_prefix=jobs_prefix, sort_target='mod')

        for value, meta in r:
            job_state, job_id = meta.key.decode('utf-8').split('/')[-2:]  # the last one is job ID
            jobs.append(dict(id=job_id.replace('id:', ''), state=job_state.split('@')[-1].replace('_jobs', '')))

    return jobs


def delete_job(owner_name, queue_id, job_id):
    # first let's get the job which has to be in a deletable state
    # we need to revisit this later, as a guiding principle we don't want delete (ie, erase history)
    jobs = []
    for s in ("queued", "suspended"):
        jobs = jobs + get_jobs(owner_name, queue_id, job_id, state=s)

    if not jobs:
        return

    job_to_be_deleted = jobs[0]

    job_name = job_to_be_deleted.get('name')
    task_names = [t for t in job_to_be_deleted.get('tasks')]
    job_key = "/jt:jess/job_queue.id:%s/job@jobs/state:%s/id:%s/name:%s/job_file" % \
              (queue_id, job_to_be_deleted.get('state'), job_id, job_name)
    task_keys = ["/jt:jess/job_queue.id:%s/job.id:%s/task@tasks/name:%s/state:queued/task_file" %
                 (queue_id, job_id, tn) for tn in task_names]

    etcd_key_deletion = [etcd_client.transactions.delete(job_key)]
    etcd_key_exist = [etcd_client.transactions.version(job_key) > 0]
    for tk in task_keys:
        etcd_key_deletion.append(etcd_client.transactions.delete(tk))
        etcd_key_exist.append(etcd_client.transactions.version(tk) > 0)

    succeeded, responses = etcd_client.transaction(
        compare=etcd_key_exist,
        success=etcd_key_deletion,
        failure=[]
    )

    if succeeded:
        return job_id
    else:
        return


def get_jobs(owner_name, queue_id, job_id=None, state=None):
    # TODO: have to make it very efficient to find job by ID, can't query all jobs from ETCD then filter the hits
    #       maybe we can search one job state at a time to get the job with supplied id
    # Job ETCD key eg: /jt:jess/job_queue.id:{queue_id}/job@jobs/state:queued/id:{job_id}
    # Using generator will definitely help avoid retrieving all jobs at once, instead we can batch by state first
    # then by job UUID first two characters, eg, 00, 01, 02 ...
    # It would be nice if we allow users to specify multiple job states
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
            return []

        jobs_prefix = '/'.join([JESS_ETCD_ROOT,
                                'job_queue.id:%s' % queue_id,
                                'job@jobs/state:%s' % ('' if state is None else state + '/')])
        r = etcd_client.get_prefix(key_prefix=jobs_prefix, sort_target='mod')

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

            tasks = get_tasks_by_job_id(queue_id, job['id'])
            job.update(tasks)
            jobs.append(job)

        if jobs:
            return jobs
        else:
            return []


def get_tasks_by_job_id(queue_id, job_id):
    tasks_prefix = '/'.join([JESS_ETCD_ROOT,
                             'job_queue.id:%s' % queue_id,
                             'job.id:%s' % job_id,
                             'task@tasks/name:'])

    r1 = etcd_client.get_prefix(key_prefix=tasks_prefix)

    tasks = dict()
    G = nx.DiGraph()
    root = {''}
    for value, meta in r1:
        k = meta.key.decode('utf-8').replace('/'.join([JESS_ETCD_ROOT,
                                                       'job_queue.id:%s' % queue_id,
                                                       'job.id:%s/task@tasks/' % job_id]), '', 1)
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
        # TODO: need to deal with gather step that depends on scatter step
        if current_task == '':  # '' is the root node, ie, a virtual task
            continue
        task_state = tasks.get(current_task).get('state')
        if task_state not in task_lists:
            task_lists[task_state] = []

        task_lists[task_state].append(
            {current_task: tasks.get(current_task)}
        )

    return {
        'tasks': tasks,
        'tasks_by_state': task_lists
    }


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

        # TODO:
        # job JSON should use the one from execution plan which may included addition parameter injected by the planner
        # job_json = job_with_execution_plan.get('job_file')

        if job_with_execution_plan:
            job_json['id'] = str(uuid.uuid4())
            job_name = job_json['name'] if job_json.get('name') else '_unnamed'
            # TODO: make this multiple key put operation transactional
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
    if len(job.get('tasks_by_state').get('completed', [])) == len(job.get('tasks')):
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

    running_task = [t for t in job.get('tasks') if job.get('tasks').get(t).get('state') == 'running']
    old_task_keys = ["/jt:jess/job_queue.id:%s/job.id:%s/task@tasks/name:%s/state:running/task_file" %
                 (queue_id, job_id, rt) for rt in running_task]

    task_key_exist_compare = []
    task_key_update = []
    for otk in old_task_keys:
        new_task_key = otk.replace('state:running', 'state:%s' % new_state)
        task_key_exist_compare.append(etcd_client.transactions.version(otk) > 0)
        task_key_update.append(etcd_client.transactions.delete(otk))  # delete old key

        task_r = etcd_client.get(otk)
        task_value = task_r[0].decode("utf-8")
        task_key_update.append(etcd_client.transactions.put(new_task_key, task_value))  # delete old key

    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(job_etcd_key_old) > 0,
            etcd_client.transactions.version(exec_job_etcd_key_old) > 0,
        ] + task_key_exist_compare,
        success=[
            etcd_client.transactions.delete(job_etcd_key_old),
            etcd_client.transactions.put(job_etcd_key_new, job_etcd_value_new),
            etcd_client.transactions.delete(exec_job_etcd_key_old),
            etcd_client.transactions.put(exec_job_etcd_key_new, ''),
        ] + task_key_update,
        failure=[]
    )

    return 'Job %s' % new_state


def resume_job(owner_name, queue_id, job_id, executor_id=None, user_id=None, node_id=None):
    return reset_job(owner_name, queue_id, job_id, new_state='resume', \
              executor_id=executor_id, user_id=user_id, node_id=node_id)


def reset_job(owner_name, queue_id, job_id, new_state='queued', executor_id=None, user_id=None, node_id=None):
    resetable_states = ('cancelled', 'suspended', 'failed', 'running', 'resume')  # careful about reset 'running' job

    if not new_state in ('queued', 'resume'):
        return

    # find the job first in one of the resetable states
    job = None
    for st in resetable_states:
        jobs = get_jobs(owner_name, queue_id, job_id, st)
        if jobs:
            job = jobs[0]
            break

    if not job:
        return

    if job.get('state') == 'suspended' and new_state == 'resume':  # suspended job can not resume, can be reset
        return

    if job.get('state') == 'suspended':
        return change_job_state(owner_name=owner_name,
                            queue_id=queue_id,
                            job_id=job_id,
                            old_state='suspended',
                            new_state='queued')

    # now find the executor(s) that worked on this job
    # there could be multiple executors per job, but for now we assume just one
    executors = get_executors(owner_name, queue_id, node_id)
    if not executors:
        return

    executor = None
    for exe in executors:
        exec_job_key = '/'.join([JESS_ETCD_ROOT,
                                 'executor.id:%s' % exe.get('id'),
                                 'job@%s_jobs' % job.get('state'),
                                 'id:%s' % job.get('id')
                                 ])

        v, kmeta = etcd_client.get(exec_job_key)
        if v is not None:
            executor = exe
            break

    if not executor:
        return

    etcd_key_exist = []
    etcd_key_delete = []
    etcd_key_add = []

    # get exec key
    exec_job_key = '/'.join([JESS_ETCD_ROOT,
                         'executor.id:%s' % executor.get('id'),
                         'job@%s_jobs' % job.get('state'),
                         'id:%s' % job.get('id')
                         ])
    etcd_key_exist.append(etcd_client.transactions.version(exec_job_key) > 0)
    etcd_key_delete.append(etcd_client.transactions.delete(exec_job_key))

    if new_state == 'resume':  # only for when it's for resume
        exec_job_key_new = exec_job_key.replace('/job@%s_jobs/' % job.get('state'), '/job@%s_jobs/' % new_state)
        etcd_key_add.append(etcd_client.transactions.put(exec_job_key_new, ''))  # empty value

    job_key = '/'.join([JESS_ETCD_ROOT,
                        'job_queue.id:%s' % queue_id,
                        'job@jobs',
                        'state:%s' % job.get('state'),
                        'id:%s' % job.get('id'),
                        'name:%s' % job.get('name'),
                        'job_file'
                        ])
    etcd_key_exist.append(etcd_client.transactions.version(job_key) > 0)
    etcd_key_delete.append(etcd_client.transactions.delete(job_key))

    job_r = etcd_client.get(job_key)
    job_etcd_value = job_r[0].decode("utf-8")
    job_key_new = job_key.replace('/job@jobs/state:%s/' % job.get('state'), '/job@jobs/state:%s/' % new_state)
    etcd_key_add.append(etcd_client.transactions.put(job_key_new, job_etcd_value))

    # we need to reset the 'input' section for each of the tasks that are to be set back to 'queued'
    queues = get_queues(owner_name, queue_id=queue_id)

    if queues:  # should only have one queue with the specified ID
        workflow_owner = queues[0].get('workflow_owner.name')
        workflow_name = queues[0].get('workflow.name')
        workflow_version = queues[0].get('workflow.ver')
    else:
        return   # need better exception handle

    job_with_execution_plan = get_job_execution_plan(workflow_owner, workflow_name, workflow_version, json.loads(job_etcd_value))

    task_original_input = {}
    for t in job_with_execution_plan.get('tasks'):
        task_name = t.get('task')
        task_input = t.get('input')
        task_original_input[task_name] = task_input

    # now get all tasks
    for t in job.get('tasks'):
        # change tasks to 'queued' that are non-completed tasks for 'resume'
        # everything to 'queued' for 'reset'
        if new_state == 'queued' or \
                (new_state == 'resume' and job.get('tasks').get(t).get('state') != 'completed'):
            task_key = '/'.join([JESS_ETCD_ROOT,
                                 'job_queue.id:%s' % queue_id,
                                 'job.id:%s' % job.get('id'),
                                 'task@tasks/name:%s' % job.get('tasks').get(t).get('name'),
                                 'state:%s' % job.get('tasks').get(t).get('state'),
                                 'task_file'
                                 ])

            task_r = etcd_client.get(task_key)
            task_etcd_value = task_r[0].decode("utf-8")

            # reset the task input to original
            task_dict = json.loads(task_etcd_value)
            task_dict['input'] = task_original_input.get(task_dict.get('task'))
            task_etcd_value = json.dumps(task_dict)

            task_key_new = task_key.replace('state:%s' % job.get('tasks').get(t).get('state'), 'state:queued')

            etcd_key_exist.append(etcd_client.transactions.version(task_key) > 0)
            if job.get('tasks').get(t).get('state') != 'queued':  # if the old state is 'queued', no key to delete
                etcd_key_delete.append(etcd_client.transactions.delete(task_key))
            etcd_key_add.append(etcd_client.transactions.put(task_key_new, task_etcd_value))

    succeeded, responses = etcd_client.transaction(
        compare=etcd_key_exist,
        success=etcd_key_delete + etcd_key_add,
        failure=[]
    )

    if succeeded:
        return "Job: %s set to '%s'" % (job_id, new_state)
    else:
        return


def suspend_job(owner_name, queue_id, job_id):
    return change_job_state(owner_name=owner_name,
                            queue_id=queue_id,
                            job_id=job_id,
                            old_state='queued',
                            new_state='suspended')


def change_job_state(owner_name, queue_id, job_id, old_state, new_state):
    if not ((old_state == 'queued' and new_state == 'suspended') or \
            (old_state == 'suspended' and new_state == 'queued')):
        return

    jobs = get_jobs(owner_name, queue_id, job_id, old_state)

    if not jobs:
        return
    else:
        job = jobs[0]

    job_key = '/'.join([JESS_ETCD_ROOT,
                        'job_queue.id:%s' % queue_id,
                        'job@jobs',
                        'state:%s' % job.get('state'),
                        'id:%s' % job.get('id'),
                        'name:%s' % job.get('name'),
                        'job_file'
                        ])

    job_r = etcd_client.get(job_key)
    job_etcd_value = job_r[0].decode("utf-8")
    job_key_new = job_key.replace('/job@jobs/state:%s/' % job.get('state'), '/job@jobs/state:%s/' % new_state)

    succeeded, responses = etcd_client.transaction(
        compare=[etcd_client.transactions.version(job_key) > 0],
        success=[etcd_client.transactions.delete(job_key), etcd_client.transactions.put(job_key_new, job_etcd_value)],
        failure=[]
    )

    if succeeded:
        return "Job: %s set from '%s' to '%s'" % (job_id, old_state, new_state)
    else:
        return
