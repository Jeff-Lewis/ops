import os
import re

from setuptools import setup
from setuptools import find_packages


tests_require = [
    'mock==0.7.2',
    'nose==1.1.2',
]

setup(
    name='ops',
    version=(
        re
        .compile(r".*__version__ = '(.*?)'", re.S)
        .match(open(os.path.join('fabfile', '__init__.py')).read())
        .group(1)
    ),
    description='Infrastructure Ops',
    author='',
    author_email='',
    url='https://github.com/balanced/ops',
    install_requires=[
        'fabric>=1.8,<2.0',
        'crontab==0.18',
        'requests==2.2.1'
    ],
    packages=find_packages(),
    tests_require=tests_require,
    scripts=[
        'scripts/archive_bucketed_logs.py',
        'scripts/archive_ossec_logs.py',
        'scripts/backup_db.py',
        'scripts/check_last_run.py',
        'scripts/s3.py',
    ],
    include_package_data=True,
    test_suite='nose.collector',
    zip_safe=False,
    extras_require={
        'test': tests_require,
    }
)
