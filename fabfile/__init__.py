from __future__ import unicode_literals

__version__ = '1.0.0'

from fabric.api import env
from chef.fabric import chef_roledefs

import es
import logs
import geoip


env.roledefs = chef_roledefs()
