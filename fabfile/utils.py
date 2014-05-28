from __future__ import unicode_literals

from fabric.api import env
import boto.ec2

def schedule(crontab):
    def annotate_function(func):
        setattr(func, 'schedule', crontab)
        return func
    return annotate_function


def find_hosts(pattern, region='us-west-1'):
    matches = []
    conn = boto.ec2.connect_to_region(region)
    reservations = conn.get_all_instances()
    for reservation in reservations:
        for instance in reservation.instances:
            if pattern in instance.tags.get('Name', ''):
                matches.append(instance)
    return [
        match.ip_address or match.private_ip_address
        for match in matches
    ]


def find_host(pattern):
    # this info comes from aws, so assuming that all hosts are alive
    return find_hosts(pattern)[0]
