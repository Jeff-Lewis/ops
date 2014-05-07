from email.mime.text import MIMEText
import logging
import os
import re
import smtplib
import socket

import fabric.operations

from boto import ec2
from boto.ec2 import elb
from infra.util import StdHook


logger = logging.getLogger(__name__)


class FabricOperations(object):
    sudo = staticmethod(fabric.operations.sudo)

    put = staticmethod(fabric.operations.put)

    local = staticmethod(fabric.operations.local)

    run = staticmethod(fabric.operations.run)

    get = staticmethod(fabric.operations.get)


class DryOperations(object):

    @staticmethod
    def sudo(input_, *args, **kwargs):
        print '    sudo -> %s, %s, %s' % (input_, args, kwargs)

    @staticmethod
    def put(input_, *args, **kwargs):
        print '    put -> %s, %s, %s' % (input_, args, kwargs)

    @staticmethod
    def local(input_, *args, **kwargs):
        print '    local -> %s, %s, %s' % (input_, args, kwargs)

    @staticmethod
    def run(input_, *args, **kwargs):
        print '    run -> %s, %s, %s' % (input_, args, kwargs)

    @staticmethod
    def get(input_, *args, **kwargs):
        print '    get -> %s, %s, %s' % (input_, args, kwargs)


IGNORE_FILE_NAMES = [
    '.gitignore'
]


def push_manifest(manifest_dir, env):

    tmp_dir = os.path.join('/tmp', os.path.basename(manifest_dir))

    def cleanup():
        env.ops.run('rm -rf {}*'.format(tmp_dir))

    env.ops.local('zip -r manifest.zip {}'.format(manifest_dir))
    logger.info('deploying')
    cleanup()
    env.ops.put('manifest.zip', '/tmp/manifest.zip')
    env.ops.run('cd /tmp && unzip manifest.zip')
    for root, dirs, files in os.walk(manifest_dir):
        folder = root[len(manifest_dir):]
        _init_directories(dirs, folder, env)
        _copy_files(tmp_dir, files, folder, env)
    cleanup()
    env.ops.local('rm manifest.zip')


def _init_directories(dirs, folder, env):
    for directory in dirs:
        rooted_dir = os.path.join('/', folder, directory)
        env.ops.sudo('[ -d {0} ] || mkdir -p {0}'.format(rooted_dir))


def _copy_files(root, files, folder, env):
    for file_name in files:
        if file_name in IGNORE_FILE_NAMES:
            logging.debug('skipping %s', os.path.join(folder, file_name))
            continue
        rooted_file = os.path.join(folder, file_name)
        remote_file = os.path.join(root, folder[1:], file_name)
        env.ops.sudo('mv {} {}'.format(remote_file, rooted_file))


def pull_manifest(manifest_dir, env):
    manifest_name = os.path.basename(manifest_dir)
    tmp_manifest_dir = os.path.join('/tmp', manifest_name)
    env.ops.run('[ -d {0} ] || mkdir {0}'.format(tmp_manifest_dir))

    for root, _, file_names in os.walk(manifest_dir):
        remote_root = os.path.join(root[len(manifest_dir):])
        remote_tmp_root = os.path.join(
            tmp_manifest_dir, root[len(manifest_dir) + 1:])
        env.ops.run('mkdir -p {}'.format(remote_tmp_root))
        for file_name in file_names:
            if file_name in IGNORE_FILE_NAMES:
                continue
            remote_file = os.path.join(remote_root, file_name)
            remote_tmp_file = os.path.join(remote_tmp_root, file_name)
            env.ops.sudo('cp {} {}'.format(remote_file, remote_tmp_file))
    env.ops.sudo('chown {0}:{0} {1} -R'.format(env.user, tmp_manifest_dir))
    env.ops.run('cd /tmp && zip -r {0}.zip {0}/'.format(manifest_name))
    zip_path = '/tmp/{0}.zip'.format(manifest_name)
    env.ops.get(zip_path, zip_path)
    env.ops.local('cd /tmp && unzip {}.zip'.format(manifest_name))

    for root, _, file_names in os.walk(tmp_manifest_dir):
        dst_root = os.path.join(
            manifest_dir, os.path.join(root[len(tmp_manifest_dir) + 1:]))
        env.ops.local('mkdir -p {}'.format(dst_root))
        for file_name in file_names:
            if file_name in IGNORE_FILE_NAMES:
                continue
            tmp_file = os.path.join(root, file_name)
            dst_file = os.path.join(dst_root, file_name)
            env.ops.local('cp {} {}'.format(tmp_file, dst_file))

    env.ops.local('rm -rf {}*'.format(tmp_manifest_dir))


class ActionSpec(object):

    def __init__(self, spec_file, task, env):
        self.spec_file = spec_file
        self.task = task
        self.env = env

    def __enter__(self):
        for task in self.load('pre-' + self.task):
            self.env.ops.sudo(task)

    def __exit__(self, type_, value, traceback):
        if type_:
            return
        for task in self.load('post-' + self.task):
            self.env.ops.sudo(task)

    def load(self, action):
        with open(self.spec_file, 'r') as fo:
            return self.parse_deploy_spec(fo, action)

    @staticmethod
    def parse_deploy_spec(fo, action):
        actions = []
        is_interesting = False
        interesting_prefix = '%{}'.format(action)
        for line in fo:
            line = line.replace('\n', '').strip()
            if is_interesting:
                if line.startswith('%'):
                    is_interesting = False
                    continue
                if line and not line.startswith('#'):
                    actions.append(line)
            else:
                is_interesting = line.startswith(interesting_prefix)
        return actions


class Report(object):
    def __init__(self, fab_env, to_addrs, smtp_host, smtp_creds):
        self.std_hook = StdHook()
        self.std_hook.attach()
        self.fab_env = fab_env
        self.to_addrs = to_addrs
        self.smtp_host = smtp_host
        self.smtp_creds = smtp_creds

    def __call__(self):
        self.std_hook.detach()

        if not self.fab_env.report:
            return

        from_id = os.getenv('LOGNAME') or os.getlogin()
        from_addr = '%s@%s' % (from_id, socket.gethostname())

        # FIXME: this is probably wrong
        log = self.std_hook.log.getvalue()
        log = re.sub('\r.+?\n', '\n', log)

        # TODO: make it pretty?
        msg = MIMEText(log)
        msg['Subject'] = '%s@%s' % (
            self.fab_env.command, ','.join(self.fab_env.roles))
        msg['From'] = from_addr
        msg['To'] = ', '.join(self.to_addrs)

        server = smtplib.SMTP(*self.smtp_host)
        try:
            server.login(*self.smtp_creds)
            server.sendmail(from_addr, self.to_addrs, msg.as_string())
        finally:
            server.quit()


def git_log(repo, target_dir, branch=None, commit=None):
    begin, end = '', commit or '@{u}'
    with fabric.api.cd(target_dir):
        fabric.api.run('git fetch')
        return fabric.api.run(
            'git --no-pager log --summary {0}..{1}'.format(begin, end)
            )


def get_instance(region_name, aws_creds, instance_name):
    ec2_cxn = ec2.connect_to_region(region_name, **aws_creds)
    instances = ec2_cxn.get_all_instances(filters={'tag:Name': instance_name})
    if not instances:
        pass
    if len(instances) > 1:
        pass
    return instances[0].instances[0]


def load_balancer_register_instance(
        region_name, aws_creds, lb_name, instance_name):
    instance = get_instance(region_name, aws_creds, instance_name)
    elb_cxn = elb.connect_to_region(region_name, **aws_creds)
    elb_cxn.register_instances(lb_name, [instance.id])


def load_balancer_deregister_instance(
        region_name, aws_creds, lb_name, instance_name):
    instance = get_instance(region_name, aws_creds, instance_name)
    elb_cxn = elb.connect_to_region(region_name, **aws_creds)
    elb_cxn.deregister_instances(lb_name, [instance.id])
