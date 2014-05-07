#!/usr/bin/env python
"""
Archives rotated OSSEC logs to S3.
"""
from datetime import datetime, timedelta
import logging
from optparse import OptionParser
import os
import re
import sys

from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from infra.util import get_aws_creds_file, get_aws_creds_env


logger = logging.getLogger()

USAGE = """\
{s3-bucket-name} [{ossec-logs-path-1} ... {ossec-logs-path-n}]
"""


def get_logs(base_path):
    rotated_logs = []
    for root, dirs, files in os.walk(base_path):
        for file in files:
            match = re.match('(?P<name>ossec-\w+?)-(?P<day>\d+)\.log\.gz',
                             file)
            if not match:
                logger.debug('%s is not a rotated ossec log, skipping', file)
                continue
            logger.debug('%s is rotated ossec log, processing', file)
            name = match.group('name')
            day = match.group('day')
            dir = root
            month = os.path.basename(dir)
            dir = os.path.dirname(dir)
            year = os.path.basename(dir)
            ts = datetime.strptime('-'.join([year, month, day]), '%Y-%b-%d')
            log = OSSECRotatedLog(
                      name + '.log.gz',
                      ts,
                      os.path.join(root, file))
            rotated_logs.append(log)
    return rotated_logs


class OSSECRotatedLog(object):
    expired_delta = timedelta(days=30)

    def __init__(self, name, ts, path):
        self.name = '_'.join([ts.strftime('%Y%m%d'), name])
        self.ts = ts
        self.path = path

    @property
    def expired(self):
        return self.ts < (datetime.now() - self.expired_delta)

    def archive(self, s3_bucket):
        with open(self.path, 'r') as fo:
            s3_key = Key(s3_bucket)
            s3_key.key = self.name
            logger.info('uploading %s to %s', fo.name, s3_key.key)
            s3_key.set_contents_from_file(fo)

    def is_archived(self, s3_bucket):
        return s3_bucket.get_key(self.name)

    def remove(self):
        logger.debug('removing %s', self.path)
        os.remove(self.path)


def main():
    opt_parser = OptionParser(usage=USAGE)
    opt_parser.add_option(
        '-v', '--verbose', action='store_true', default=False)
    opt_parser.add_option(
        '-a', '--aws-creds', default=None)
    opts, args = opt_parser.parse_args()
    if not args:
        raise Exception(USAGE)

    if opts.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    if opts.aws_creds:
        aws_access_key, aws_secret_key = get_aws_creds_file(opts.aws_creds)
    else:
        aws_access_key, aws_secret_key = get_aws_creds_env()

    s3_bucket_name = args[0]
    base_paths = args[1:]
    s3_cxn = S3Connection(aws_access_key, aws_secret_key)
    s3_bucket = Bucket(s3_cxn, s3_bucket_name)
    for base_path in args:
        logger.debug('getting rotated ossec logs in %s', base_path)
        for log in get_logs(base_path):
            if not log.is_archived(s3_bucket):
                log.archive(s3_bucket)
            elif log.expired:
                log.remove()

if __name__ == '__main__':
    main()
