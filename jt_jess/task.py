import re
import json
import etcd3

from .exceptions import ParentTaskError

from .job import get_jobs
from .job import get_jobs_by_executor
from .job import update_job_state

from .queue import get_queues

from .config import ETCD_HOST
from .config import ETCD_PORT
from .config import JESS_ETCD_ROOT


# TODO: we will need to have better configurable settings for these other parameters
etcd_client = etcd3.client(host=ETCD_HOST, port=ETCD_PORT,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None,
           user=None, password=None)


def has_next_task(owner_name, queue_id, executor_id):
    # first find all running jobs for the executor
    jobs = get_jobs_by_executor(owner_name, queue_id, executor_id, state='running')
    job_ids = [j.get('id') for j in jobs]

    # if one task is in queued state, return true
    for job_id in job_ids:
        job = get_jobs(owner_name, queue_id, job_id, state='running')[0]
        if job.get('tasks_by_state').get('queued', []):
            return True

    return False


def next_task(owner_name, queue_id, executor_id, job_id, job_state='running'):
    # TODO: verify executor already registered under the job queue
    #       get endpoint for executor has not implemented yet

    # find candidate job(s)

    # let assume it's for running jobs by the same executor, this disables running the same job by
    # multiple executors, this advanced feature may need to be supported later

    # if the client ask for task from queued jobs, will make sure queue is in 'open' state
    if job_state == 'queued':
        queues = get_queues(owner_name, queue_id=queue_id)
        if queues:
            if queues[0].get('state') != 'open':
                return  ## not open, then not give new task from queued jobs
        else:
            return  # should never happen

    jobs = get_jobs(owner_name, queue_id, job_id=job_id, state=job_state)

    # jobs run by the current executor
    if job_state == 'running':
        job_ids = [j.get('id') for j in get_jobs_by_executor(owner_name, queue_id, executor_id, job_state)]
        new_jobs = [j for j in jobs if j['id'] in job_ids]
        jobs = new_jobs
    else:  # start new job, but let's check whether there are jobs to be resumed
        resume_jobs = get_jobs_by_executor(owner_name, queue_id, executor_id, 'resume')
        for rj in reversed(resume_jobs):
            rjobs = get_jobs(owner_name, queue_id, job_id=rj.get('id'), state='resume')
            jobs = (rjobs if rjobs else []) + jobs   # add resumable jobs at beginning

    if not jobs:
        return

    for job in jobs:
        for task in job.get('tasks_by_state', {}).get('queued', []):
            task_to_be_scheduled = list(task.values())[0]
            task_to_be_scheduled['job.id'] = job.get('id')

            # using transaction to update job state and task state, return task only when it's success
            # 1) if job key exists: /jt:jess/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job@jobs/state:queued/id:d66b3f18-834a-4129-9d4c-9af975afee44/name:first_job/job_file
            # 2) if task key exists: /jt:jess/queue.id:fef43d38-5097-4028-9671-71ad7c7e42d9/job.id:d66b3f18-834a-4129-9d4c-9af975afee44/task@tasks/name:prepare_metadata_xml/state:queued/task_file
            # 2.5) look for depends_on tasks (if any), and copy over their output to current task's input
            # 3) then create new job key and task key, and delete old ones
            # if precondition fails, return None, ie, no task returned
            job_etcd_key = '/'.join([
                JESS_ETCD_ROOT,
                'job_queue.id:%s' % queue_id,
                'job@jobs',
                'state:%s' % job.get('state'),
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
                    for d_task_name in job.get('tasks'):
                        if not (d_task_name == dt_name or dt_name.startswith('%s.' % d_task_name)):
                            continue

                        if job.get('tasks').get(d_task_name).get('state') != 'completed':
                            dependency_ready = False
                            break

                    if not dependency_ready:  # if one is not ready, no need to check more
                        break

            if not dependency_ready:  # if the current task's dependency is not satisfied
                continue

            # TODO: check current task for parameters depending on parent tasks, fetch output from parent tasks as needed
            task_file = _populate_task_input_params(job, task_file)

            job_r = etcd_client.get(job_etcd_key)
            job_file = job_r[0].decode("utf-8")

            new_job_etcd_key = job_etcd_key.replace('/state:%s/' % job.get('state'), '/state:running/')  # if match
            new_task_etcd_key = task_etcd_key.replace('/state:queued/', '/state:running/')

            # add running job to executor
            # /jt:jess/executor.id:d4b319ad-08df-4c0b-9a31-e061e97a7b93/job@running_jobs/id:0d912b9d-565e-4330-8c36-7b727c8d10a4
            exec_job_etcd_key = '/'.join([
                JESS_ETCD_ROOT,
                'executor.id:%s' % executor_id,
                'job@running_jobs',
                'id:%s' % job.get('id')
            ])
            exec_job_etcd_value = ''
            exec_job_etcd_key_resume = exec_job_etcd_key.replace('/job@running_jobs/', '/job@resume_jobs/')

            if job_state == 'running':  # no need to change job file
                etcd_client.transaction(
                    compare=[
                        etcd_client.transactions.version(job_etcd_key) > 0,  # test key exists
                        etcd_client.transactions.version(task_etcd_key) > 0,  # test key exists
                    ],
                    success=[
                        etcd_client.transactions.put(exec_job_etcd_key, exec_job_etcd_value),
                        etcd_client.transactions.put(new_task_etcd_key, task_file),
                        etcd_client.transactions.delete(exec_job_etcd_key_resume),  # delete the resume key if exists
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )
            #elif job.get('state') in ('resume'):  # resuming a job
            #    pass
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
                        etcd_client.transactions.delete(exec_job_etcd_key_resume),  # delete the resume key if exists
                        etcd_client.transactions.delete(task_etcd_key),
                    ],
                    failure=[]
                )

            task_to_be_scheduled['state'] = 'running'
            task_to_be_scheduled['task_file'] = task_file  # updated task_file
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

    task = job.get('tasks').get(task_name)

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


def _populate_task_input_params(job, task_file):
    task = json.loads(task_file)
    # iterate through parent tasks
    for key in task.get('input', {}):  # a task may not have input
        value = task.get('input').get(key)
        if not isinstance(value, str):
            continue

        m = re.search(r"{{([\w.]+)@([\w.]+)}}", value)
        if m:
            pkey = m.group(1)  # parent output key
            ptask = m.group(2)  # parent task name, TODO: for 'gather' task, this will refer to multiple tasks

            ptask_file_str = job.get('tasks').get(ptask, {}).get('task_file', '{}')
            ptask_output = json.loads(ptask_file_str).get('output', [])
            if ptask_output:
                # get the last output since a task may have run multiple times
                if pkey in ptask_output[-1]:
                    new_value = ptask_output[-1].get(pkey)  # handle 'gather' task later
                else:
                    raise ParentTaskError("Parent task '%s' in job '%s' misses output parameter '%s'" % (
                        ptask, job.get('id'), pkey
                    ))
            else:  # this should never happens
                raise ParentTaskError("Parent task '%s' in job '%s' misses output" % (ptask, job.get('id')))

            task['input'][key] = new_value

    return json.dumps(task)
