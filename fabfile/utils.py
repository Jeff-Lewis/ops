from __future__ import unicode_literals


def schedule(crontab):
    def annotate_function(func):
        setattr(func, 'schedule', crontab)
        return func
    return annotate_function