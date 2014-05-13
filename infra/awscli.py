import os

from fabric.api import run, settings
from fabric.context_managers import shell_env
from fabric.operations import sudo


class _AWSCli(object):

    def __init__(self, credentials=None):
        self._credentials = credentials

    def reconfigure(self, credentials):
        self._credentials = credentials

    @property
    def credentials(self):
        creds = {
            'AWS_DEFAULT_REGION': 'us-east-1',
            'AWS_ACCESS_KEY_ID': '',
            'AWS_SECRET_ACCESS_KEY': ''
        }
        if not self._credentials:
            for k in creds.keys():
                creds[k] = os.environ.get(k, creds.get(k))

        return creds

    @staticmethod
    def ensure_awscli_installed():
        with settings(warn_only=True):
            result = run('aws --version')
            if result.succeeded:
                return
            result = sudo('pip install awscli')
            if result.succeeded:
                return
            raise EnvironmentError('AWS CLI must be installed via hosts')

    def __call__(self, cmd, as_sudo=False):
        if not cmd.startswith('aws '):
            cmd = 'aws ' + cmd

        executor = sudo if as_sudo else run
        with shell_env(**self.credentials):
            return executor(cmd)
