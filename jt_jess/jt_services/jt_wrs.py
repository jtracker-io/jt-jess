import json
import requests
from ..exceptions import WRSNotAvailable, WorklowNotFound
from ..config import WRS_URL


def get_workflow_by_name(workflow_owner_name, workflow_name, workflow_version=None):
    # /workflows/owner/{owner_name}/workflow/{workflow_name}/ver/{workflow_version}
    request_url = '%s/workflows/owner/%s/workflow/%s' % \
                  (WRS_URL.strip('/'), workflow_owner_name, workflow_name)
    if workflow_version:
        request_url += '/ver/%s' % workflow_version
    try:
        r = requests.get(request_url)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        full_workflow_name = "%s.%s" % (workflow_owner_name, workflow_name)
        if workflow_version:
            full_workflow_name += '/' + workflow_version
        raise WorklowNotFound(full_workflow_name)

    return json.loads(r.text)


def get_workflow_by_id(workflow_id, workflow_version=None):
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


def get_job_execution_plan(owner_name, workflow_name, workflow_verion, job_json):
    request_url = '%s/workflows/owner/%s/workflow/%s/ver/%s/job_execution_plan' % (WRS_URL.strip('/'),
                                                                                   owner_name, workflow_name,
                                                                                   workflow_verion)
    try:
        r = requests.put(request_url, json=job_json)
    except:
        raise WRSNotAvailable('WRS service temporarily unavailable')

    if r.status_code != 200:
        raise WorklowNotFound("%s.%s/%s" % (owner_name, workflow_name, workflow_verion))

    return json.loads(r.text)

