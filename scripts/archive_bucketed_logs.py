#!/usr/bin/env python
"""
Archives {host}/{date} bucketed logs to S3.
"""
from datetime import timedelta, datetime
import glob
import logging
from optparse import OptionParser
import os
import shutil
import subprocess
import sys
import tempfile

from boto.s3.bucket import Bucket
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from infra.util import get_aws_creds_file, get_aws_creds_env


logger = logging.getLogger()


USAGE = """\
{s3-bucket-name} [{log-buckets-path-1} ... {log-buckets-path-n}]
"""


def get_host_buckets(base_path):
    buckets = []
    for host_path in glob.glob(os.path.join(base_path, '*')):
        if not os.path.isdir(host_path):
            logger.debug('%s in %s is not a host bucket', host_path, base_path)
            continue
        host_name = os.path.basename(host_path)
        buckets.append((host_name, host_path))
    return buckets


def get_ts_buckets(host_name, host_path, ts_fmt='%Y-%m-%d'):
    buckets = []
    for ts_path in glob.glob(os.path.join(host_path, '*')):
        if not os.path.isdir(ts_path):
            logger.debug('%s in %s is not a host bucket',
                         ts_path, host_path)
            continue
        try:
            ts = datetime.strptime(os.path.basename(ts_path), ts_fmt)
        except ValueError, ex:
            logger.debug('%s in %s is not a host bucket',
                         ts_path, host_path)
            continue
        buckets.append((host_name, ts, ts_path))
    return buckets


def get_logs(base_path):
    return sorted([BucketedLog(host_name, ts, ts_path)
        for host_name, host_path in get_host_buckets(base_path)
        for host_name, ts, ts_path in get_ts_buckets(host_name, host_path)],
        key=lambda x: x.ts)


class BucketedLog(object):

    REAP_THRESHOLD = timedelta(days=15)
    RIPE_THRESHOLD = timedelta(days=1)

    def __init__(self, host, ts, path):
        self.host = host
        self.ts = ts
        self.path = path
        self.name = '_'.join([ts.strftime('%Y%m%d'), self.host]) + '.tar.gz'

    @property
    def expired(self):
        return self.ts < (datetime.now() - self.REAP_THRESHOLD)

    @property
    def ripe(self):
        return self.ts < (datetime.now() - self.RIPE_THRESHOLD)

    def archive(self, s3_bucket):
        with BucktedLogArchiver(self) as ar:
            s3_key = Key(s3_bucket)
            s3_key.key = os.path.basename(self.name)
            logger.info('uploading %s to %s', ar.fo.name, s3_key.key)
            s3_key.set_contents_from_file(ar.fo)

    def is_archived(self, s3_bucket):
        return s3_bucket.get_key(self.name)

    def remove(self):
        logger.debug('removing %s', self.path)
        shutil.rmtree(self.path)


class BucktedLogArchiver(object):
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        self.path = os.path.join(tempfile.gettempdir(), self.log.name)
        logger.debug('creating archive %s from %s',
                     self.path, self.log.path)
        cmd = [
            'tar',
            'zcf',
            self.path,
            os.path.basename(self.log.path),
            ]
        proc = subprocess.Popen(
                   cmd,
                   stderr=sys.stderr,
                   cwd=os.path.dirname(self.log.path))
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                'command "%s" failed, code - %d, err - %s' % (
                    ' '.join(cmd),
                    proc.returncode,
                    err))
        self.fo = open(self.path, 'r')
        return self

    def __exit__(self, type, value, traceback):
        if self.fo:
            self.fo.close()
            self.fo = None
        if os.path.isfile(self.path):
            logger.debug('deleting archive %s', self.path)
            os.remove(self.path)


def main():
    opt_parser = OptionParser(usage=USAGE)
    opt_parser.add_option(
        '-v', '--verbose', action='store_true', default=False)
    opt_parser.add_option(
        '-a', '--aws-creds', default=None)
    opt_parser.add_option(
        '--reap-threshold', default=None, type="int",
        help='Age in days after which to remove bucket.')
    opt_parser.add_option(
        '--ripe-threshold', default=None, type="int",
        help='Age in days after which to archive bucket.')
    opts, args = opt_parser.parse_args()
    if not args:
        raise Exception(USAGE)

    if opts.reap_threshold is not None:
        BucketedLog.REAP_THRESHOLD = timedelta(days=opts.reap_threshold)
    if opts.ripe_threshold is not None:
        BucketedLog.RIPE_THRESHOLD = timedelta(days=opts.ripe_threshold)

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
        logger.debug('getting buckets in %s', base_path)
        for log in get_logs(base_path):
            if not log.ripe:
                logger.debug('%s is not ripe, skipping', log.path)
            elif not log.is_archived(s3_bucket):
                log.archive(s3_bucket)
            elif log.expired:
                logger.debug('%s is expired, removing', log.path)
                log.remove()

if __name__ == '__main__':
    main()
