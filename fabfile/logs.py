from __future__ import unicode_literals
import datetime
import logging
import os
import sys

from fabric.api import task, run, hosts
from fabfile.utils import schedule, find_hosts
from fabric.decorators import roles

from fabric_rundeck import cron

from infra import awscli

logger = logging.getLogger(__name__)


class ArgumentError(ValueError):
    pass


# archive_bucketed_logs.py \
#    balanced.log /mnt/logs/ --aws-creds=/home/deploy/.aws_creds --verbose

aws = awscli._AWSCli()


@cron('30 * * * *')
@hosts(*find_hosts('log-prod'))
@task
def archive(s3_bucket_name='balanced.log',
            paths='/mnt/log/',
            reap_threshold='15',
            ripe_threshold='1',
            aws_credentials=None,
            verbose=False):
    """
    Archives {host}/{date} bucketed logs to S3. Thresholds are in day units.

    """
    # arguments are passed as string
    reap_threshold = int(reap_threshold)
    ripe_threshold = int(ripe_threshold)
    if not paths or not paths.split(','):
        raise ArgumentError(
            r"paths should be a string -- separated by commas "
            "like so: 'path-1\,path-2\,...\,path-n'"
        )
    setup_logging(verbose)
    paths = paths.split(',')
    logger.debug('Paths: %s', paths)
    ripe_threshold = datetime.timedelta(days=ripe_threshold)
    reap_threshold = datetime.timedelta(days=reap_threshold)
    # assumes that there's an aws cli on the machine.
    aws.ensure_awscli_installed()
    aws.reconfigure(aws_credentials)
    for path in paths:
        logger.debug('getting buckets in %s', path)
        for log in get_logs(path, reap_threshold, ripe_threshold):
            if not log.ripe:
                logger.debug('%s is not ripe, skipping', log.path)
            elif not log.is_archived(s3_bucket_name):
                log.archive(s3_bucket_name)
            elif log.expired:
                logger.debug('%s is expired, removing', log.path)
                log.remove()


def setup_logging(verbose):
    logger.setLevel(logging.WARNING)
    if verbose:
        logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler(sys.stderr))


def get_logs(base_path, reap_threshold, ripe_threshold, ts_fmt='%Y-%m-%d'):
    if not base_path.endswith('/'):
        base_path += '/'

    cmd = 'find {base_path} -maxdepth 2 -type d'.format(base_path=base_path)
    output = run(cmd)
    paths = output.split('\r\n')
    logger.debug('Paths: %s', len(paths))
    bucketed_logs = []
    for path in paths:
        truncated_path = path.replace(base_path, '')
        if not truncated_path:
            # truncated_path == base path, so ignore it
            continue

        split_path = truncated_path.split('/')
        if len(split_path) == 1:
            # it's just a hostname, move on
            continue

        host, ts = split_path
        try:
            ts = datetime.datetime.strptime(ts, ts_fmt)
        except ValueError:
            logger.exception(
                '%s in %s is not valid log fmt (%s)',
                path, host, ts_fmt
            )
            continue

        bl = BucketedLog(host, ts, path, reap_threshold, ripe_threshold)
        bucketed_logs.append(bl)

    bucketed_logs.sort(key=lambda bucket: bucket.ts)
    logger.debug('Number of bucketed logs: %s', len(bucketed_logs))
    return bucketed_logs


class BucketedLog(object):

    def __init__(self, host, ts, path,
                 reap_threshold=datetime.timedelta(days=15),
                 ripe_threshold=datetime.timedelta(days=1)):
        self.host = host
        self.ts = ts
        self.path = path
        self.reap_threshold = reap_threshold
        self.ripe_threshold = ripe_threshold

    @property
    def name(self):
        return '_'.join([self.ts.strftime('%Y%m%d'), self.host]) + '.tar.gz'

    @property
    def expired(self):
        return self.ts < (datetime.datetime.utcnow() - self.reap_threshold)

    @property
    def ripe(self):
        return self.ts < (datetime.datetime.utcnow() - self.ripe_threshold)

    def is_archived(self, s3_bucket):
        logger.debug('Is %s in bucket: %s?', self.name, s3_bucket)
        output = aws('s3 ls s3://{bucket_name}/{name}'.format(
            bucket_name=s3_bucket,
            name=self.name
        ))
        rval = output.strip()
        logger.debug('output: %s', rval)
        return rval

    def archive(self, s3_bucket):
        remote_archive = os.path.join('/tmp', self.name)
        loc = 's3://{bucket}/{key}'.format(bucket=s3_bucket,
                                           key=os.path.basename(self.name))

        logger.info('tarring up %s into %s', self.path, remote_archive)
        run('tar zcf {dest} {src}'.format(dest=remote_archive, src=self.path))
        logger.info('uploading %s to %s', remote_archive, s3_bucket)
        aws('s3 mv {src} {dest}'.format(src=remote_archive, dest=loc))
        logger.debug('deleting archive %s', remote_archive)
        run('rm -fr {}'.format(remote_archive))

    def remove(self):
        logger.debug('removing %s', self.path)
        run('rm -fr {}'.format(self.path))
