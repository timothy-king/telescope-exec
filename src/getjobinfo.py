#!/usr/bin/env python

import starexecparser
import starexecpipe


parser = starexecparser.StarExecParser('Download a completed getjobinfo file')
parser.addJobId()
parser.addStarExecCredentials()
parser.addOptionalZipFile()

parser.processArgs()
se_creds    = parser.getStarExecCredentials()
job_id      = parser.getJobId()
zipfile     = parser.getZipFile()

if zipfile is None:
    zipfile='Job'+str(job_id)+'_info.zip'


with starexecpipe.StarExecPipe(se_creds) as pipe:
    with open(zipfile, 'w') as zf:
        pipe.getjobinfo(job_id, zipfile, True)
