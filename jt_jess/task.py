import uuid
import json
import etcd3

from .job import get_jobs
from .job import get_jobs_by_executor
from .job import update_job_state

from .config import JESS_ETCD_HOST
from .config import JESS_ETCD_PORT
from .config import JESS_ETCD_ROOT


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=JESS_ETCD_HOST, port=JESS_ETCD_PORT,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None)


def has_next_task(owner_name, queue_id, executor_id):
    # first find all running jobs for the executor
    jobs = get_jobs_by_executor(owner_name, queue_id, executor_id, state='running')
    job_ids = [j.get('id') for j in jobs]

    # if one task is in queued state, return true
    for job_id in job_ids:
        job = get_jobs(owner_name, queue_id, job_id)[0]
        if job.get('tasks_by_state').get('queued', []):
            return True

    return False


def next_task(owner_name, queue_id, executor_id, job_id, job_state='running'):
    # TODO: verify executor already registered under the job queue
    #       get endpoint for executor has not implemented yet

    # find candidate job(s)

    # let assume it's for running jobs by the same executor, this disables running the same job by
    # multiple executors, this advanced feature may need to be supported later

    jobs = get_jobs(owner_name, queue_id, job_id=job_id, state=job_state)
    if not jobs:
        return

    # jobs run by the current executor
    if job_state == 'running':
        job_ids = [j.get('id') for j in get_jobs_by_executor(owner_name, queue_id, executor_id, job_state)]
        new_jobs = [j for j in jobs if j['id'] in job_ids]
        jobs = new_jobs

    if not jobs:
        return

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
                'state:%s' % job_state,
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
            if depends_on:
                for dt in depends_on:
                    dt_name = dt.split('@')[1]

                    # latter is for 'gather' task
                    for d_task_name in job.get('tasks_by_name'):
                        if not (d_task_name == dt_name or dt_name.startswith('%s.' % d_task_name)):
                            continue

                        if job.get('tasks_by_name').get(d_task_name).get('state') != 'completed':
                            dependency_ready = False
                            break

                    if not dependency_ready:  # if one is not ready, no need to check more
                        break

            if not dependency_ready:  # if the current task's dependency is not satisfied
                continue

            # TODO: check current task for parameters depending on parent tasks, fetch output from parent tasks as needed

            job_r = etcd_client.get(job_etcd_key)
            job_file = job_r[0].decode("utf-8")

            new_job_etcd_key = job_etcd_key.replace('/state:queued/', '/state:running/')  # if match
            new_task_etcd_key = task_etcd_key.replace('/state:queued/', '/state:running/')

            # add running job to executor
            # /jthub:jes/executor.id:d4b319ad-08df-4c0b-9a31-e061e97a7b93/job@running_jobs/id:0d912b9d-565e-4330-8c36-7b727c8d10a4
            exec_job_etcd_key = '/'.join([
                JESS_ETCD_ROOT,
                'executor.id:%s' % executor_id,
                'job@running_jobs',
                'id:%s' % job.get('id')
            ])
            exec_job_etcd_value = ''

            if job_state == 'running':  # no need to change job file
                etcd_client.transaction(
                    compare=[
                        etcd_client.transactions.version(job_etcd_key) > 0,  # test key exists
                        etcd_client.transactions.version(task_etcd_key) > 0,  # test key exists
                    ],
                    success=[
                        etcd_client.transactions.put(exec_job_etcd_key, exec_job_etcd_value),
                        etcd_client.transactions.put(new_task_etcd_key, task_file),
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )
            else:  # need to change job file key by adding new and removing old
                etcd_client.transaction(
                    compare=[
                        etcd_client.transactions.version(job_etcd_key) > 0,  # test key exists
                        etcd_client.transactions.version(task_etcd_key) > 0,  # test key exists
                    ],
                    success=[
                        etcd_client.transactions.put(exec_job_etcd_key, exec_job_etcd_value),
                        etcd_client.transactions.put(new_job_etcd_key, job_file),
                        etcd_client.transactions.delete(job_etcd_key),
                        etcd_client.transactions.put(new_task_etcd_key, task_file),
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )

            task_to_be_scheduled['state'] = 'running'
            return task_to_be_scheduled


def end_task(owner_name, queue_id, executor_id, job_id, task_name, result, success):
    if success:
        end_state = 'completed'
    else:
        end_state = 'failed'

    jobs = get_jobs(owner_name, queue_id, job_id, 'running')
    # print(jobs)
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
        task_dict = json.loads(task_file)
        if 'output' not in task_dict:
            task_dict['output'] = []
        task_dict['output'].append(result)

    except:
        # raise TaskNotFound or TaskNotInRunningState
        return

    # print(task)
    # print(task_file)

    # TODO: verify executor is the same as expected

    # TODO: update task_file with result reported by executor

    # write the updated task_file back
    new_task_etcd_key = task_etcd_key.replace('/state:running/', '/state:%s/' % end_state)

    etcd_client.transaction(
        compare=[
            etcd_client.transactions.version(task_etcd_key) > 0,
        ],
        success=[
            etcd_client.transactions.put(new_task_etcd_key, json.dumps(task_dict)),
            etcd_client.transactions.delete(task_etcd_key),
        ],
        failure=[]
    )

    task['state'] = end_state

    # update job state after task state change
    rv = update_job_state(owner_name, queue_id, executor_id, job_id)

    # TODO: it should be good to log job state change
    #if rv:
    #    print(rv)

    return task
