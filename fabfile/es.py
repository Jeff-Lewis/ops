from __future__ import unicode_literals
from datetime import date
from datetime import datetime
from datetime import timedelta
import logging
import urlparse
import requests

from fabric.api import task, run
from fabfile.utils import schedule

from fabric_rundeck import cron

logger = logging.getLogger(__name__)


@cron('0 11 * * *')
@task
def optimize(target=None, base_url='http://localhost:9200'):
    """Optimize ES index

    """
    indexes = []
    # get today toordinal from remote
    today_toordinal = int(run(
        'python -c "import datetime; '
        'print datetime.date.today().toordinal()"'
    ))
    # default to optimize yesterday log
    if target is None:
        yesterday = datetime.date.fromordinal(today_toordinal - 1)
        yesterday_str = yesterday.strftime('%Y%m%d')
        monthonly = yesterday.strftime('%Y%m')
        logger.info('Optimizing yesterday %s logs', yesterday)
        indexes.append('log-{}'.format(yesterday_str))
        indexes.append('dash-log-{}'.format(monthonly))
    # process all indexes except today
    elif target == 'all':
        today = datetime.date.fromordinal(today_toordinal)
        today_str = today.strftime('%Y%m%d')
        logger.info('Optimizing indexes of all but not today %s', today)
        indices = run('ls /mnt/search/elasticsearch/elasticsearch/nodes/0/indices').split()
        indices = filter(lambda index: not index.endswith(today_str), indices)
        indexes.extend(indices)
    # process the given target
    else:
        logger.info('Optimizing index %s', target)
        indexes.append(target)
    for index in indexes:
        url = urlparse.urljoin(
            base_url,
            '/{}/_optimize?max_num_segments=2'.format(index),
        )
        logger.debug('POSTing to %s', url)
        run('curl -XPOST {}'.format(url))


@cron('0 11 * * *')
@task
def purge_outdated(max_age_days=45):
    """Purge outdated logs"""
    if max_age_days < 30:
        raise Exception("ERROR: Refusing to delete logs less than 30 days old")

    d = date.today() - timedelta(days=max_age_days)
    cutoff = "log-{}".format(d.strftime('%Y%m%d'))
    indexes = []
    indices = run('ls /mnt/search/elasticsearch/elasticsearch/nodes/0/indices').split()
    indices = filter(lambda index: index < cutoff, indices)
    indexes.extend(indices)
    # remove each old index from es
    errors = False
    for i in indices:
        uri = "http://localhost:9200/{}".format(i)
        logger.info('Deleting %s index from es', i)
        r = requests.delete(uri)
        if r.status_code != requests.codes.ok:
            errors = True
            logger.info("Unable to delete %s index from es".format(i))

    if errors:
        raise Exception("Errors occurred while deleting indexes!")
