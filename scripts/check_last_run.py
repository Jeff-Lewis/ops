#!/usr/bin/env python
"""
Checks if last run time is greater than threshold.
"""
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import re
import sys


logger = logging.getLogger()

USAGE = """\
{last-run-file} {threshold}\
"""

TIMESTAMP_FMT = '%Y-%m-%dT%H:%M:%S'

DELTA_RE = r'(?P<value>\d+)(?P<unit>s|sec|m|min|h|hr|d|day)'


def _parse_delta(delta_str):
    match = re.match(DELTA_RE, delta_str)
    if not match:
        raise ValueError('Invalid delta %s' % delta_str)
    value = int(match.group('value'))
    unit = match.group('unit')
    if unit in ('s', 'sec'):
        delta = timedelta(seconds=value)
    elif unit in ('m', 'min'):
        delta = timedelta(minutes=value)
    elif unit in ('h', 'hr'):
        delta = timedelta(hours=value)
    elif unit in ('d', 'day'):
        delta = timedelta(days=value)
    return delta


def check(last_run_path, threshold_str):
    timestamp_str = open(last_run_path, 'r').read().strip()
    timestamp = datetime.strptime(timestamp_str, TIMESTAMP_FMT)
    threshold = _parse_delta(threshold_str)
    delta = datetime.now() - timestamp
    return delta <= threshold, timestamp, delta


def main():
    opt_parser = OptionParser(usage=USAGE)
    opt_parser.add_option(
        '-v', '--verbose', action='store_true', default=False)
    opts, args = opt_parser.parse_args()
    if not args:
        raise Exception(USAGE)

    if opts.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    if not args:
        lines = sys.stdin
    else:
        lines = [' '.join(args)]
    for line in lines:
        line = line.split()
        if len(line) != 2:
            raise ValueError('Invalid line %s' % line)
        last_run_path, threshold_str = line
        ok, timestamp, delta = check(last_run_path, threshold_str)
        if not ok:
            print >>sys.stderr, 'Last run %s for %s is %s second(s) ago' % (
                timestamp, last_run_path, delta.seconds)


if __name__ == '__main__':
    main()
