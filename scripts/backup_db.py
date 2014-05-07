#!/usr/bin/env python
"""
Backs up database to S3.
"""
from datetime import datetime
import glob
import logging
from optparse import OptionParser
import os
import subprocess
import tempfile
from multiprocessing.pool import ThreadPool
import sys

from boto.s3.bucket import Bucket
from boto.s3.bucketlistresultset import BucketListResultSet
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import iso8601
from infra.util import get_aws_creds_file, get_aws_creds_env


logger = logging.getLogger(__name__)

USAGE = """\
{s3-bucket} {database} {user-name}
"""

MAX_UPLOAD_SIZE = 5 * 1024 * 1000000 # ~5GB


class DumpDB(object):
    date_fmt = '%Y%m%d_%H%M%S'
    compression_level = '9'
    format = 'custom'

    def __init__(self, host, db, username, temp_dir=None):
        self.host = host
        self.db = db
        self.username = username
        self.temp_dir = temp_dir or tempfile.gettempdir()

    def __enter__(self):
        self.timestamp = datetime.now().strftime(self.date_fmt)
        self.tmp_path = os.path.join(
            self.temp_dir, self.db + '-' + self.timestamp + '.sql')

        logger.info('dumping %s to %s', self.db, self.tmp_path)
        cmd = [
            'pg_dump',
            '--format=' + self.format,
            '--compress=' + self.compression_level,
            '--file=' + self.tmp_path,
            '--user=' + self.username,
            '--host=' + self.host,
            '--exclude-table=' + 'repl_test',
            self.db,
            ]
        proc = subprocess.Popen(cmd)
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(
                'dump command "%s" failed, code - %d, err - %s' % (
                ' '.join(cmd), proc.returncode, err))
        return self

    def __exit__(self, type, value, traceback):
        if os.path.isfile(self.tmp_path):
            logger.debug('deleting dump %s', self.tmp_path)
            os.unlink(self.tmp_path)


def archive(s3_bucket, dump_file, dst_file):
    s3_key = Key(s3_bucket)
    logger.info('uploading %s to s3 bucket %s as %s',
        dump_file, s3_bucket.name, dst_file)
    s3_key.key = dst_file
    with open(dump_file, 'rb') as dump_fo:
        s3_key.set_contents_from_file(dump_fo)


def archive_in_parts(s3_bucket, dump_file, key_name):
    def split_file(file_path, key):
        temp_dir = tempfile.gettempdir()
        prefix = os.path.join(temp_dir, key)
        cl = ['split', '-b {}'.format(MAX_UPLOAD_SIZE), file_path, prefix]
        subprocess.check_call(cl)
        return sorted(glob.glob('{}*'.format(prefix))), temp_dir
    mp = s3_bucket.initiate_multipart_upload(key_name)
    chunks, temp_dir = split_file(dump_file, key_name)
    pool = ThreadPool(len(chunks))
    pool.map(spawn_upload, [(mp, chunk, i) for i, chunk in enumerate(chunks)])
    mp.complete_upload()
    for chunk in chunks:
        os.unlink(chunk)


def spawn_upload((multipart_upload, path_to_chunk, index)):
    multipart_upload.upload_part_from_file(open(path_to_chunk, 'rb'), index+1)


def reap(s3_bucket, capacity, dry=False):
    keys = [key for key in BucketListResultSet(s3_bucket)]
    if len(keys) <= capacity:
        return 0
    keys = sorted(keys, key=lambda x: iso8601.parse_date(x.last_modified))
    keys.reverse()
    for key in keys[capacity:]:
        logger.debug("deleting key %s last modified @ %s from s3 bucket %s",
            key.name, key.last_modified, s3_bucket.name)
        if not dry:
            key.delete()
    return len(keys) - capacity


def main():
    opt_parser = OptionParser(usage=USAGE)
    opt_parser.add_option(
        '-v', '--verbose', action='store_true', default=False)
    opt_parser.add_option(
        '-d', '--dry', action='store_true', default=False)
    # TODO: if you do more intelligent backup naming this is not needed
    #       e.g. hrmod48, daymod14, year+month, etc.
    opt_parser.add_option(
        '-c', '--capacity-count', type='int', default=24 * 365)
    opt_parser.add_option(
         '--host', default='localhost')
    opt_parser.add_option(
        '-a', '--aws-creds', default=None)
    opts, args = opt_parser.parse_args()
    if args:
        lines = [' '.join(args)]
    else:
        lines = sys.stdin

    if opts.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    if opts.aws_creds:
        aws_access_key, aws_secret_key = get_aws_creds_file(opts.aws_creds)
    else:
        aws_access_key, aws_secret_key = get_aws_creds_env()

    s3_cxn = S3Connection(aws_access_key, aws_secret_key)
    for line in lines:
        parts = line.strip().split()
        if len(parts) == 3:
            s3_bucket_name, db, username = parts
        else:
            raise Exception(USAGE)
        with DumpDB(opts.host, db, username) as dump:
            s3_bucket = Bucket(s3_cxn, s3_bucket_name)
            if os.path.getsize(dump.tmp_path) > MAX_UPLOAD_SIZE:
                archive_in_parts(s3_bucket, dump.tmp_path, dump.timestamp + '.sql')
            else:
                archive(s3_bucket, dump.tmp_path, dump.timestamp + '.sql')
            reap(s3_bucket, opts.capacity_count, opts.dry)

if __name__ == '__main__':
    main()
