#!/usr/bin/env python
"""
Simple operations on an S3 bucket. Add more as needed. For use with shell.

e.g.
s3.py list my.bucket --format='%(n)s %(m)s' | sort -k 2 | tail -n1 | cut -f1 -d' ' | s3.py download my.bucket > latest.key
s3.py list some.bucket --before=30d | s3.py delete some.bucket --verbose
s3.py list some.db --prefix=some_db_production | while read l; do echo $l ${l:19}; done | s3.py rename some.db --verbose
"""
import argparse
from datetime import datetime, timedelta
import logging
import os
import re
import sys

from boto.s3.bucketlistresultset import bucket_lister
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import iso8601
from infra.util import get_aws_creds_file, get_aws_creds_env


logger = logging.getLogger(__name__)


# commands

def list_command(s3_bucket, args):
    logger.debug('listing bucket %s using prefix "%s"',
                 s3_bucket.name, args.prefix)
    for key in bucket_lister(s3_bucket, prefix=args.prefix):
        last_modified = (iso8601
            .parse_date(key.last_modified)
            .replace(tzinfo=None))
        if args.after and last_modified < args.after:
            logger.debug('discarding key %s', key.name)
            continue
        if args.before and args.before < last_modified:
            logger.debug('discarding key %s', key.name)
            continue
        print args.format % {
            'bucket': s3_bucket.name,
            'b': s3_bucket.name,
            'name': key.name,
            'n': key.name,
            'last_modified': key.last_modified,
            'lm': key.last_modified,
            'modified': key.last_modified,
            'm': key.last_modified,
        }


def download_command(s3_bucket, args):
    line_no = 0
    for line in args.input:
        line_no += 1
        key_name = line.strip()
        if not args.dir_path:
            if not args.dry:
                output = sys.stdout
            logger.debug('downloading %s key %s to stdout',
                s3_bucket.name, key_name)
        else:
            file_path = os.path.join(dir_path, key_name)
            logger.debug('downloading %s key %s to %s',
                s3_bucket.name, key_name, file_path)
            if not dry:
                output = open(file_path, 'w')
        key = s3_bucket.get_key(key_name)
        if not key:
            raise ValueError('{} has no key {}'.format(
                      s3_bucket.name, key_name))
        if not args.dry:
            try:
                key.get_contents_to_file(output)
            finally:
                if args.dir_path:
                    output.close()


def delete_command(s3_bucket, args):
    line_no = 0
    for line in args.input:
        line_no += 1
        key_name = line.strip()
        logger.debug('delete %s key %s', s3_bucket.name, key_name)
        key = s3_bucket.get_key(key_name)
        if not args.dry:
            key.delete()


def rename_command(s3_bucket, args):
    line_no = 0
    for line in args.input:
        line_no += 1
        key_names = map(lambda x: x.strip(), line.strip().split())
        if len(key_names) != 2:
            raise ValueError(
                'line #%s invalid, expecting source and destination key name '
                'pair' % line_no)
        src_key_name, dst_key_name = key_names
        logger.debug('renaming %s key %s to %s',
            s3_bucket.name, src_key_name, dst_key_name)
        key = s3_bucket.get_key(src_key_name)
        if not key:
            raise ValueError('{} has no key {}'.format(
                      s3_bucket.name, src_key_name))
        if not args.dry:
            key.copy(s3_bucket.name, dst_key_name)
            key.delete()


def upload_command(s3_bucket, args):
    line_no = 0
    for line in args.input:
        line_no += 1
        line = line.strip()
        src_path, _, dst_path = line.partition(' ')
        if not dst_path:
            dst_path = os.path.basename(src_path)
        key = Key(s3_bucket, dst_path)
        logger.debug('uploading %s %s key from %s',
            s3_bucket.name, dst_path, src_path)
        if not args.dry:
            key.set_contents_from_filename(src_path)
            if args.public:
                key.set_acl('public-read')


# main

class TimestampAction(argparse.Action):

    def __call__(self, parser, namespace, values, option_string=None):
        timestamps = []
        match = re.match("""\
(?P<sign>[+-]{0,1})(?P<value>\d+)(?P<unit>d|day|h|hr|m|mins|sec)\
""",
            values.lower())
        if match:
            sign = 1 if match.group('sign') == '+' else -1
            value = int(match.group('value'))
            unit = match.group('unit')
            if unit in ('d', 'day'):
                delta = timedelta(days=value)
            elif unit in ('h', 'hr'):
                delta = timedelta(hours=value)
            elif unit in ('s', 'sec'):
                delta = timedelta(seconds=value)
            ts = datetime.utcnow() + sign * delta
        else:
            ts = iso8601.parse_date(values)
        setattr(namespace, self.dest, ts)


def create_arg_parser():
    # common
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        '-v', '--verbose', action='store_true', default=False)
    common.add_argument(
        '-s', '--spew', action='store_true', default=False)
    common.add_argument(
        '-c', '--aws-creds', metavar='FILE', default=None)
    parents = [common]

    # root
    root = argparse.ArgumentParser(parents=parents)
    subs = root.add_subparsers(title='sub-commands')

    # list
    sub_command = subs.add_parser('list',
        description='List files in S3-BUCKET-NAME.',
        parents=parents)
    sub_command.add_argument('s3_bucket_name',
        nargs=1, metavar='S3-BUCKET-NAME', help='S3 bucket.')
    sub_command.add_argument(
        '-o', '--format', default='%(name)s')
    sub_command.add_argument(
        '-p', '--prefix', default=None)
    sub_command.add_argument(
        '-b', '--before', default=None, action=TimestampAction)
    sub_command.add_argument(
        '-a', '--after', default=None, action=TimestampAction)
    sub_command.set_defaults(command=list_command)

    # download
    sub_command = subs.add_parser('download',
        description="""\
Downloads file(s) from S3-BUCKET-NAME. File names are read from stdin one \
per line.\
""",
        parents=parents)
    sub_command.add_argument('s3_bucket_name',
        nargs=1, metavar='S3-BUCKET-NAME', help='S3 bucket.')
    sub_command.add_argument('dir_path',
        nargs='?', metavar='DIRECTORY',
        help='DIRECTORY to place downloaded files.',
        default=None)
    sub_command.add_argument(
        '-d', '--dry', action='store_true', default=False)
    sub_command.set_defaults(command=download_command, input=sys.stdin)

    # delete
    sub_command = subs.add_parser('delete',
        description="""\
Deletes file(s) from S3-BUCKET-NAME. File names are read from stdin one per \
line.\
""",
        parents=parents)
    sub_command.add_argument('s3_bucket_name',
        nargs=1, metavar='S3-BUCKET-NAME', help='S3 bucket.')
    sub_command.add_argument(
        '-d', '--dry', action='store_true', default=False)
    sub_command.set_defaults(command=delete_command, input=sys.stdin)

    # rename
    sub_command = subs.add_parser('rename',
        description="""\
Renames file(s) in S3-BUCKET-NAME. The source and destination file names are \
read from stdin one pair per line.\
""",
        parents=parents)
    sub_command.add_argument('s3_bucket_name',
        nargs=1, metavar='S3-BUCKET-NAME', help='S3 bucket.')
    sub_command.add_argument(
        '-d', '--dry', action='store_true', default=False)
    sub_command.set_defaults(command=rename_command, input=sys.stdin)

    # upload
    sub_command = subs.add_parser('upload',
        description='Uploads file(s) to S3-BUCKET-NAME.',
        parents=parents)
    sub_command.add_argument('s3_bucket_name',
        nargs=1, metavar='S3-BUCKET-NAME', help='S3 bucket.')
    sub_command.add_argument(
        '-d', '--dry', action='store_true', default=False)
    sub_command.add_argument(
        '-p', '--public', action='store_true', default=False)
    sub_command.add_argument(
        '--create-bucket', help='Create the S3 bucket if necessary',
        action='store_true', default=False)
    sub_command.set_defaults(command=upload_command, input=sys.stdin)

    return root


def main():
    # command line
    arg_parser = create_arg_parser()
    args = arg_parser.parse_args()

    # logging
    logging.getLogger() if args.spew else logger
    if args.verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler(sys.stderr))

    # connection
    if args.aws_creds:
        aws_access_key, aws_secret_key = get_aws_creds_file(args.aws_creds)
    else:
        aws_access_key, aws_secret_key = get_aws_creds_env()
    logger.debug('creating connection')
    s3_cxn = S3Connection(aws_access_key, aws_secret_key)

    # bucket
    if args.command.func_name == 'upload_command' and args.create_bucket:
        logger.debug('getting/creating bucket %s', args.s3_bucket_name[0])
        s3_bucket = s3_cxn.create_bucket(args.s3_bucket_name[0])
    else:
        logger.debug('getting bucket %s', args.s3_bucket_name[0])
        s3_bucket = s3_cxn.get_bucket(args.s3_bucket_name[0])

    # do it
    args.command(s3_bucket, args)


if __name__ == '__main__':
    main()
