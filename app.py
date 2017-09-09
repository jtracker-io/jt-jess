#!/usr/bin/env python3
import connexion
import datetime
import logging
import jt_jess
from jt_jess.exceptions import OwnerNameNotFound, AMSNotAvailable, WorklowNotFound, WRSNotAvailable
from connexion import NoContent


def get_jobs(owner_name, queue_id, job_id=None, state=None):
    return jt_jess.get_jobs(owner_name, queue_id, job_id, state) or ('No job found', 404)


def get_job(owner_name, queue_id, job_id, state=None):
    jobs = get_jobs(owner_name, queue_id, job_id, state)
    if jobs:
        return jobs[0]
    else:
        return 'No job found', 404


def enqueue_job(owner_name, queue_id, jobjson):
    return jt_jess.enqueue_job(owner_name, queue_id, jobjson)


def get_executors(owner_name, queue_id):
    pass


def get_executors1(owner_name, queue_id):
    pass


def get_executors2(owner_name, queue_id):
    pass


def get_executor(owner_name, queue_id, executor_id):
    pass


def register_executor(owner_name, queue_id):
    pass


def next_task(owner_name, queue_id, executor, job_id=None):
    return jt_jess.next_task(owner_name, queue_id, executor, job_id)


def next_task_from_job(owner_name, queue_id, executor, job_id):
    return next_task(owner_name, queue_id, executor, job_id)


def complete_task(owner_name, queue_id, job_id, task_name, result):
    return jt_jess.complete_task(owner_name, queue_id, job_id, task_name, result)


def fail_task(owner_name, queue_id, job_id, task_name, result):
    pass


def get_queues(owner_name, workflow_name=None, workflow_version=None):
    if workflow_name and '.' in workflow_name:
        if len(workflow_name.split('.')) > 2:
            return 'Value for workflow name parameter can not have more than two dots (.)', 400
        else:
            workflow_owner_name, workflow_name = workflow_name.split('.')
    else:
        workflow_owner_name = owner_name

    try:
        workflows = jt_jess.get_queues(owner_name, workflow_name, workflow_version, workflow_owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow job queue found', 404)


def get_queues1(owner_name):
    try:
        workflows = get_queues(owner_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow job queue found', 404)


def get_queues2(owner_name, workflow_name):
    try:
        workflows = get_queues(owner_name, workflow_name)
    except OwnerNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500
    except WorklowNotFound as err:
        return str(err), 404
    except WRSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow job queue found', 404)


def get_queues3(queue_id):
    pass


def get_queues4(owner_name, queue_id):
    pass


def get_job_summary(owner_name, queue_id):
    pass


def job_action(owner_name):
    pass


def queue_action(owner_name):
    pass


def executor_action(owner_name, queue_id, executor_id):
    return


def register_queue(owner_name, owner_type='org'):
    exists = jt_jess.get_owner(owner_name)
    if exists:
        return NoContent, 409
    else:
        return jt_jess.create_owner(owner_name, owner_type)


def register_queue1():
    pass


def get_tasks(owner_name):
    pass


logging.basicConfig(level=logging.INFO)
app = connexion.App(__name__)
app.add_api('swagger.yaml', base_path='/api/jt-jess/v0.1')
# set the WSGI application callable to allow using uWSGI:
# uwsgi --http :8080 -w app
application = app.app

if __name__ == '__main__':
    # run our standalone gevent server
    app.run(port=1208, server='gevent')
