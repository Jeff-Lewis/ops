from __future__ import unicode_literals
import datetime
import logging
import os
import sys
import hashlib
import requests

from fabric.api import task, run
from fabfile.utils import schedule

from fabric_rundeck import cron

from infra import awscli

logger = logging.getLogger(__name__)

GEO_DATABASE_URL = "http://geolite.maxmind.com/download/geoip/database/GeoLiteCity.dat.gz"
S3_BUCKET_NAME = "balanced.geoip"

aws = awscli._AWSCli()


def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, 'r+b') as f:
        for block in iter(lambda: f.read(blocksize), ''):
            hash.update(block)
    return hash.hexdigest()


@cron('0 4 7 * *')
@task
def update(url=GEO_DATABASE_URL,
           s3_bucket=S3_BUCKET_NAME,
           aws_credentials=None):
    """
    Update GeoLiteCity Database
    """
    geoip_archive = os.path.join('/tmp', 'GeoLiteCity.dat.gz')
    geoip_loc = 's3://{bucket}/{key}'.format(bucket=s3_bucket,
                                             key=os.path.basename(geoip_archive))
    md5sums_archive = os.path.join('/tmp', 'md5sums')
    md5sums_loc = 's3://{bucket}/{key}'.format(bucket=s3_bucket,
                                               key=os.path.basename(md5sums_archive))
    # download archive
    with open(geoip_archive, 'wb') as handle:
        response = requests.get(GEO_DATABASE_URL, stream=False)

        if not response.ok:
            raise Exception("Unable to download database!")

        for block in response.iter_content(1024):
            if not block:
                break

            handle.write(block)

    # write md5sum and name to md5sums file
    with open(md5sums_archive, 'wb') as handle:
        handle.write("{md5} {name}".format(md5=md5sum(md5sums_archive),
                                           name=os.path.basename(geoip_archive)))

    # assumes that there's an aws cli on the machine.
    aws.ensure_awscli_installed()
    aws.reconfigure(aws_credentials)

    aws('s3 mv {src} {dest}'.format(src=geoip_archive, dest=geoip_loc))
    aws('s3 mv {src} {dest}'.format(src=md5sums_archive, dest=md5sums_loc))

    # cleanup
    logger.info('Cleaning up')
    try:
        os.remove(geoip_archive)
        os.remove(md5sums_archive)
    except Exception as e:
        logger.info('Unable to complete clean up')
