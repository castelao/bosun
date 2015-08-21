#!/usr/bin/env python

import os

def qstat(job_id):
    """

        ATENTION. Some headers uses two lines, like:
        ... Req'd  ... Elap
        ... Time   ... Time
        So that taking only the second line, Req'd Time and Elap Time will
          both be just Time, therefore only the last value is saved. For
          now it's sufficient since I really want the Elap Time.
    """
    data = os.popen("qstat -a %s" % job_id).read()

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
