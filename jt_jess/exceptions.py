__all__ = [
    'OwnerNameNotFound',
    'WorklowNotFound',
    'AMSNotAvailable',
    'WRSNotAvailable'
]


class OwnerNameNotFound(Exception):
    def __str__(self):
        return 'Owner name not found: %s' % (self.args[0])


class WorklowNotFound(Exception):
    def __str__(self):
        return 'Workflow not found: %s' % (self.args[0])


class AMSNotAvailable(Exception):
    def __str__(self):
        return 'Account Management Service temporarily not available'


class WRSNotAvailable(Exception):
    def __str__(self):
        return 'Workflow Registration Service temporarily not available'
