from . import job, queue, executor as exe, task
from .exceptions import OwnerNameNotFound, AMSNotAvailable, WorklowNotFound, WRSNotAvailable, QueueCreationFailure

__version__ = '0.2.0a7'


def get_jobs(owner_name, queue_id, job_id=None, state=None):
    try:
        return job.get_jobs(owner_name, queue_id, job_id, state) or ('No job found', 404)
    except Exception as err:
        return "Error: %s" % err, 400


def get_jobs_by_executor(owner_name, queue_id, executor_id, state=None):
    return job.get_jobs_by_executor(owner_name, queue_id, executor_id, state=state)


def get_job(owner_name, queue_id, job_id, state=None):
    jobs = get_jobs(owner_name, queue_id, job_id, state)
    if jobs:
        return jobs[0]
    else:
        return 'No job found', 404


def enqueue_job(owner_name, queue_id, jobjson):
    try:
        rv = job.enqueue_job(owner_name, queue_id, jobjson)
        return rv, 200
    except Exception as err:
        return 'Failed: %s' % str(err), 400


def get_executors(owner_name, queue_id=None, executor_id=None):
    return exe.get_executors(owner_name, queue_id, executor_id)


def get_executor(owner_name, queue_id, executor_id):
    return get_executors(owner_name, queue_id=queue_id, executor_id=executor_id)


def get_executor1(owner_name, executor_id):
    return get_executors(owner_name, executor_id=executor_id)


def register_executor(owner_name, queue_id, executor=None):
    if executor is None:
        executor = dict()
    if not executor.get('id'):
        return 'Invalid executor object', 400

    try:
        rv = exe.register_executor(owner_name, queue_id, executor)
        return rv, 200
    except:
        return 'Failed, please make sure same executor has not been register before', 400


def has_next_task(owner_name, queue_id, executor_id):
    return task.has_next_task(owner_name, queue_id, executor_id)


def next_task(owner_name, queue_id, executor_id, job_id=None, job_state=None):
    my_task = task.next_task(owner_name, queue_id, executor_id, job_id, job_state)
    return my_task or {}


def complete_task(owner_name, queue_id, executor_id, job_id, task_name, result):
    return task.end_task(owner_name, queue_id, executor_id, job_id, task_name, result, success=True)


def fail_task(owner_name, queue_id, executor_id, job_id, task_name, result):
    return task.end_task(owner_name, queue_id, executor_id, job_id, task_name, result, success=False)


def get_queues(owner_name, workflow_name=None, workflow_version=None, queue_id=None):
    if workflow_name and '.' in workflow_name:
        if len(workflow_name.split('.')) > 2:
            return 'Value for workflow name parameter can not have more than two dots (.)', 400
        else:
            workflow_owner_name, workflow_name = workflow_name.split('.')
    elif workflow_name:  # if we have workflow_name, otherwise no need to set it
        workflow_owner_name = owner_name
    else:
        workflow_owner_name = None

    try:
        queues = queue.get_queues(owner_name, workflow_name=workflow_name,
                                       workflow_version=workflow_version,
                                       workflow_owner_name=workflow_owner_name,
                                       queue_id=queue_id)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return queues or ('No workflow job queue found', 404)


def get_queues1(owner_name):
    try:
        queues = get_queues(owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return queues or ('No workflow job queue found', 404)


def get_queues3(owner_name, workflow_name):
    try:
        queues = get_queues(owner_name, workflow_name=workflow_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return queues or ('No workflow job queue found', 404)


def get_queues2(owner_name, queue_id=None):
    try:
        queues = get_queues(owner_name, queue_id=queue_id)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    if queue_id:
        return queues[0] if queues else ('No workflow job queue found', 404)

    return queues or ('No workflow job queue found', 404)


def get_job_summary(owner_name, queue_id):
    pass


def job_action(owner_name=None, queue_id=None, job_id=None, action=None):
    if action is None:
        action = dict()

    if action.get('action') in ('cancel', 'suspend'):
        # only 'suspended', 'queued' and 'running' jobs can be cancelled
        # no effect on a job that is 'cancelled' or 'failed'
        executor_id = action.get('executor_id')
        user_id = action.get('user_id')
        action_type = action.get('action')
        return job.stop_job(owner_name=owner_name, action_type=action_type, queue_id=queue_id,
                         job_id=job_id, executor_id=executor_id, user_id=user_id)

    else:
        return 'Not implemented yet', 200


def queue_action(owner_name):
    pass


def executor_action(owner_name, queue_id, executor_id):
    return


def register_queue(owner_name, workflow_name, workflow_version):
    if workflow_name and '.' in workflow_name:
        if len(workflow_name.split('.')) > 2:
            return 'Value for workflow name parameter can not have more than two dots (.)', 400
        else:
            workflow_owner_name, workflow_name = workflow_name.split('.')
    else:
        workflow_owner_name = owner_name

    try:
        my_queue = queue.create_queue(owner_name, workflow_name, workflow_version, workflow_owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except QueueCreationFailure as err:
        return str(err), 400
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return my_queue
