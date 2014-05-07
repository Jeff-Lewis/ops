"""
Fabric tunneling utilities
    by shn@glucose.jp

class ForwardServer and relates things are refere Robey Pointer's paramiko
example.

usage::

    with make_tunnel('user@192.168.0.2:10022') as t:
        run('pwd')

"""
import hashlib
import logging
import select
import SocketServer
#import random
import time
import threading

from fabric.api import env
from fabric.network import join_host_strings, normalize
from fabric.state import connections


__all__ = ['tunnel', 'make_tunnel']

logger = logging.getLogger(__name__)


def tunnel():
    KEY = 'tunnel_hoststring'

    if hasattr(env, KEY):
        return make_tunnel(getattr(env, KEY))
    else:
        return NullTunnel()


def make_tunnel(tunnel=None, remote=None, local_port=None,
                teardown_timeout=None):
    if remote is None:
        remote = env.host_string
    username, remote_hostname, remote_port = normalize(remote)

    if local_port is None:
        #local_port = random.randint(10000, 65535)
        # local_port = port_from_host(remote)
        local_port = 0                    # Let the OS pick

    client = connections[tunnel]

    return TunnelThread(
               remote_hostname, remote_port,
               local_port,
               client.get_transport(),
               teardown_timeout)


def port_from_host(hoststring):
    return int(hashlib.sha1(hoststring).hexdigest()[-4:], 16) | 1024


class NullTunnel():
    def __enter__(self):
        env.tunnel = self
        return self

    def __exit__(self, *exc):
        del env['tunnel']
        pass

    def rsync_shell_option(self):
        #return ''
        return env.steppingstone


class TunnelThread(threading.Thread):
    def __init__(self, remote_host, remote_port, local_port, transport,
                 teardown_timeout=None):
        threading.Thread.__init__(self)

        class SubHander (Handler):
            chain_host = remote_host
            chain_port = int(remote_port, 10)
            ssh_transport = transport

        self.server = ForwardServer(('127.0.0.1', local_port), SubHander)
        _addr, port = self.server.server_address
        self.local_port = port
        self.teardown_timeout = teardown_timeout

    def run(self):
        self.server.serve_forever()

    def __enter__(self):
        self.old_env = env.user, env.host, env.port, env.host_string
        env.host_string = join_host_strings(env.user, '127.0.0.1',
                                            self.local_port)
        env.host = '127.0.0.1'
        env.port = self.local_port

        self.start()

        env.tunnel = self

        return self

    def __exit__(self, *exc):
        if self.teardown_timeout:
            verbose('waiting %s sec(s) before shutdown' %
                    self.teardown_timeout)
            time.sleep(self.teardown_timeout)

        if env.host_string in connections:
            connections[env.host_string].close()
            del connections[env.host_string]

        self.server.shutdown()

        env.user, env.host, env.port, env.host_string = self.old_env

        del env['tunnel']

    def rsync_shell_option(self):
        return '-e "ssh -p %d -i %s"' % (self.local_port, env.key_filename)


class ForwardServer(SocketServer.ThreadingTCPServer):
    daemon_threads = False
    allow_reuse_address = True


class Handler(SocketServer.BaseRequestHandler):
    def handle(self):
        request_peername = self.request.getpeername()

        try:
            chan = self.ssh_transport.open_channel('direct-tcpip',
                                                   (self.chain_host,
                                                    self.chain_port),
                                                   request_peername)
        except Exception, e:
            verbose('Incoming request to %s:%d failed: %s' % (self.chain_host,
                                                              self.chain_port,
                                                              repr(e)))
            return
        if chan is None:
            verbose(('Incoming request to %s:%d was rejected by the SSH server'
                     '.') %
                    (self.chain_host, self.chain_port))
            return

        verbose('Connected!  Tunnel open %r -> %r -> %r' % (request_peername,
                                                            chan.getpeername(),
                                                            (self.chain_host,
                                                             self.chain_port)))
        while True:
            r, w, x = select.select([self.request, chan], [], [])
            if self.request in r:
                data = self.request.recv(1024)
                if len(data) == 0:
                    break
                chan.send(data)
            if chan in r:
                data = chan.recv(1024)
                if len(data) == 0:
                    break
                self.request.send(data)
        chan.close()
        self.request.close()
        verbose('Tunnel closed from %r' % (request_peername,))


def verbose(s):
    logger.debug(s)
