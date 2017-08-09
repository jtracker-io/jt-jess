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


def get_workflow(account_name, workflow_name):
    try:
        workflow = jt_jes.get_workflow(account_name, workflow_name)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def get_workflow_ver(account_name, workflow_name, workflow_version):
    try:
        workflow = jt_jes.get_workflow(account_name, workflow_name, workflow_version)
    except AccountNameNotFound as err:
        return str(err), 404
    except AMSNotAvailable as err:
        return str(err), 500

    return workflow or ('No workflow found', 404)


def register_job_queue(account_name, account_type='org'):
    exists = jt_jes.get_account(account_name)
    if exists:
        return NoContent, 409
    else:
        return jt_jes.create_account(account_name, account_type)


def release_workflow(account_name, workflow_name, workflow_version):
    pass


def validate_job_json(account_name, workflow_name, workflow_version):
    pass


def download_workflow(account_name, workflow_name, workflow_version):
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
