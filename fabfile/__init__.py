from __future__ import unicode_literals

__version__ = '1.0.0'

from fabric.api import env

import es
import logs
import geoip


env.use_ssh_config = True
