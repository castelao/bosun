#!/usr/bin/env python

import os
import subprocess
import re
from datetime import datetime

def qstat(job_id):
    """

        ATENTION. Some headers uses two lines, like:
        ... Req'd  ... Elap
        ... Time   ... Time
        So that taking only the second line, Req'd Time and Elap Time will
          both be just Time, therefore only the last value is saved. For
          now it's sufficient since I really want the Elap Time.
    """
    data = subprocess.check_output(['qstat', '-a', job_id])

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
            # FIXME: It shouldn't be hard coded.
            header = header.replace('Memory', 'MemoryReq')
            header = header.replace('Time', 'TimeReq')
            header = header.replace('Time', 'TimeElap')

    return statuses


def extract_timestep_progress(logfile):
    """

        ATENTION, must migrate from popen to subprocess
    """
    line = os.popen('tac %s | grep -m1 yyyy' % logfile).read()
    #line = subprocess.check_output('tac %s | grep -m1 yyyy' % logfile)
    line = line.splitlines()[-1]

    current = re.search('(\d{4})/(\s*\d{1,2})/(\s*\d{1,2})\s(\s*'
            '\d{1,2}):(\s*\d{1,2}):(\s*\d{1,2})', line)

    if current:
        current = datetime(*[int(i) for i in current.groups()])

    return current
