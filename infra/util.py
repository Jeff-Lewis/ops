import logging
import os
from StringIO import StringIO
import sys


logger = logging.getLogger(__name__)


def get_aws_creds_file(creds_file):
    logger.debug('loading aws-creds from %s', creds_file)
    with open(os.path.expanduser(creds_file), 'r') as fo:
        creds = dict(
            map(lambda x: (x[0], x[2].strip()),
                [line.strip().partition('=') for line in fo]))
    if 'AWSAccessKeyId' not in creds:
        raise Exception('%s does not have AWSAccessKeyId key', creds_file)
    aws_access_key = creds['AWSAccessKeyId']
    if 'AWSSecretKey' not in creds:
        raise Exception('%s does not have AWSSecretKey key', creds_file)
    aws_secret_key = creds['AWSSecretKey']
    return aws_access_key, aws_secret_key


def get_aws_creds_env():
    logger.debug('loading aws-creds from env')
    key_keys = ['ACCESS_KEY', 'AWS_ACCESS_KEY_ID']
    secret_keys = ['SECRET_KEY', 'AWS_SECRET_ACCESS_KEY']
    if not any(key for key in key_keys if key in os.environ):
        raise Exception('no environ variable %s', 'AWS_ACCESS_KEY_ID')
    aws_access_key = [
        os.environ.get(key) for key in key_keys if os.environ.get(key)
    ][0]
    if not any(key for key in secret_keys if key in os.environ):
        raise Exception('no environ variable %s', 'AWS_SECRET_ACCESS_KEY')
    aws_secret_key = [
        os.environ.get(key) for key in secret_keys if os.environ.get(key)
    ][0]
    return aws_access_key, aws_secret_key


class StdHook(object):
    class _Hook(object):
        def __init__(self, out, log):
            self.out = out
            self.log = log

        def write(self, text):
            self.out.write(text)
            self.log.write(text)

        def flush(self):
            self.out.flush()

        def isatty(self):
            return True

    def __init__(self):
        self.log = StringIO()
        self.attached = False

    def attach(self):
        if not self.attached:
            self.stdout = sys.stdout
            sys.stdout = self._Hook(sys.stdout, self.log)
            self.stderr = sys.stderr
            sys.stderr = self._Hook(sys.stderr, self.log)
            self.attached = True

    def detach(self):
        if not self.attached:
            return False
        sys.stdout = self.stdout
        self.stdout = None
        sys.stderr = self.stderr
        self.stderr = None
        self.attached = False
        return True

    def __enter__(self):
        self.attach()

    def __exit__(self, type, value, traceback):
        self.detach()
