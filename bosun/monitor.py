#!/usr/bin/env python

import os

def qstat(job_id):
    data = os.popen("qstat -a %s" % job_id).read()

    if data == '':
        return

    statuses = {}
    header = None
    for line in data.split('\n'):
        if header and not line.startswith('----'):
            info = line.split()
            try:
                statuses[info[0]] = dict(zip(header.split()[1:], info))
            except IndexError:
                pass
        elif line.startswith('Job ID'):
            header = line
    return statuses
