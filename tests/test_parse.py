from __future__ import unicode_literals
import inspect
import pprint
import unittest

from crontab import CronTab
from fabric.main import find_fabfile, load_fabfile


def explode_cron(schedule_string):
    if not schedule_string:
        return {}

    schedule = {
        'time': {
            'seconds': '0',
            'minute': '0',
            'hour': '0',
        },
        'month': '*',
        'dayofmonth': {
            'day': '1',
        },
        'weekday': {
            'day': '*'
        },
        'year': '*'
    }

    cron = CronTab(schedule_string)
    if not cron.matchers.minute.any:
        schedule['time']['minute'] = cron.matchers.minute.input

    if not cron.matchers.hour.any:
        schedule['time']['hour'] = cron.matchers.hour.input

    if not cron.matchers.day.any:
        schedule['dayofmonth']['day'] = cron.matchers.day.input

    if not cron.matchers.month.any:
        schedule['month'] = cron.matchers.month.input

    if not cron.matchers.weekday.any:
        schedule['weekday']['day'] = cron.matchers.weekday.input

    if not cron.matchers.year.any:
        schedule['year'] = cron.matchers.year.input

    #  http://wiki.gentoo.org/wiki/Cron
    if all([cron.matchers.day.any, cron.matchers.weekday.any]):
        # if both are specified, it means every day, so remove
        # dayofmonth (since Rundeck gets confused)
        del schedule['dayofmonth']
    elif not cron.matchers.day.any and cron.matchers.weekday.any:
        del schedule['weekday']
    elif cron.matchers.day.any and not cron.matchers.weekday.any:
        del schedule['dayofmonth']

    return schedule


def visit_task(task, path):
    # Unwrap
    while hasattr(task, 'wrapped'):
        task = task.wrapped
    # Smash the closure
    if task.func_code.co_name == 'inner_decorator':
        closure = dict(zip(task.func_code.co_freevars,
                           (c.cell_contents for c in task.func_closure)))
        task = closure.get('func', closure.get('fn', task))
    args = inspect.getargspec(task)
    return {
        'name': task.func_name,
        'path': path,
        'doc': task.__doc__,
        'schedule': explode_cron(getattr(task, 'schedule', None)),
        'argspec': {
          'args': args.args,
          'varargs': args.varargs,
          'keywords': args.keywords,
          'defaults': args.defaults,
        },
    }


def visit(c, path=[]):
    ret = []
    for key, value in c.iteritems():
        if isinstance(value, dict):
            ret.extend(visit(value, path + [key]))
        else:
            ret.append(visit_task(value, path))
    return ret


class TestParseFabfile(unittest.TestCase):

    @unittest.skip('schedule not used any more')
    def test_parse_schedule_from_logs_archive(self):
        callables = load_fabfile(find_fabfile(['fabfile/logs.py']))[1]
        visited = visit(callables)
        # pprint.pprint(visited)

        for t in visited:
            if t['name'] == 'archive':
                break
        else:
            t = None

        self.assertIsNotNone(t)
        self.assertIn('schedule', t)
        self.assertItemsEqual(
            t['schedule'].keys(),
            ['month', 'time', 'weekday', 'year']
        )

class TestScheduleParsing(unittest.TestCase):

    def test_schedule_10_15_every_day(self):
        exploded = explode_cron('01 05 ? APR,MAR,MAY FRI,MON,TUE *')
        self.assertDictEqual(
            exploded, {
                'month': 'apr,mar,may',
                'time': {'hour': '05', 'minute': '01', 'seconds': '0'},
                'weekday': {'day': 'fri,mon,tue'},
                'year': '*'
            }
        )

    def test_schedule_monthly(self):
        exploded = explode_cron('@monthly')
        self.assertDictEqual(
            exploded,
            {'dayofmonth': {'day': '1'},
             'time': {
                 'hour': '0',
                 'minute': '0',
                 'seconds': '0',
             },
             'month': '*',
             'year': '*'},
            'Not expecting weekday'
        )
