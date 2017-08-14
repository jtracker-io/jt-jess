import yaml
from .workflow import Workflow


class JTracker(object):
    def __init__(self, **kwargs):
        self._workflow = Workflow(kwargs)


    @property
    def workflow(self):
        return self._workflow


    def validate_jobfile(self, jobfile):
        pass


    def generate_taskfiles(self, jobfile):
        pass

