#!/usr/bin/env python3
import connexion
import datetime
import logging
import jt_jes
from jt_jes.exceptions import AccountNameNotFound, AMSNotAvailable
from connexion import NoContent


def get_jobs(account_name):
    pass


def get_job(account_name):
    pass


def enqueue_job(account_name):
    pass


def get_workers(account_name, job_queue_id):
    pass


def get_worker(account_name, job_queue_id, worker_id):
    pass


def register_worker(account_name, job_queue_id):
    pass


def next_task(account_name, job_queue_id, worker):
    pass


def complete_task(account_name, job_queue_id, task_name):
    pass


def fail_task(account_name, job_queue_id, task_name):
    pass


def get_job_queues(account_name):
    try:
        workflows = jt_jes.get_workflows(account_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow found', 404)


def get_job_queues1(account_name):
    try:
        workflows = jt_jes.get_workflows(account_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow found', 404)


def get_job_queues2(account_name):
    try:
        workflows = jt_jes.get_workflows(account_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow found', 404)


def get_job_queues3(account_name):
    try:
        workflows = jt_jes.get_workflows(account_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflows or ('No workflow found', 404)


def get_job_summary(account_name):
    pass


def job_action(account_name):
    pass


def job_queue_action(account_name):
    pass


def worker_action(account_name):
    pass


def register_job_queue(account_name, account_type='org'):
    exists = jt_jes.get_account(account_name)
    if exists:
        return NoContent, 409
    else:
        return jt_jes.create_account(account_name, account_type)


def register_job_queue1():
    pass


def get_tasks(account_name):
    pass


logging.basicConfig(level=logging.INFO)
app = connexion.App(__name__)
app.add_api('swagger.yaml', base_path='/api/jt-jes/v0.1')
# set the WSGI application callable to allow using uWSGI:
# uwsgi --http :8080 -w app
application = app.app

if __name__ == '__main__':
    # run our standalone gevent server
    app.run(port=1208, server='gevent')
