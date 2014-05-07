from __future__ import unicode_literals
import datetime
import logging
import urlparse

from fabric.api import task, run


logger = logging.getLogger(__name__)


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
