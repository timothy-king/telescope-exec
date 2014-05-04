#!/usr/bin/env python

import starexecparser
import telescope


def toAddZippedCsvFile(te_creds, job_id, zipfile, internal, commit):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        telescope.addZippedCsvFile(dbcur, job_id, zipfile, internal)
        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()

parser = starexecparser.StarExecParser('Add results for a completed job.')
parser.addJobId()
parser.addOptionalZipFile()
parser.addTelescopeCredentials()
parser.addCommit()
parser.addOptionalInternalFile()


parser.processArgs()
job_id         = parser.getJobId()
zipfile        = parser.getZipFile()
te_creds       = parser.getTelescopeCredentials()
commit         = parser.getCommit()
internal       = parser.getOptionalInternalFile()

if zipfile is None:
    zipfile='Job'+str(job_id)+'_info.zip'

toAddZippedCsvFile(te_creds, job_id, zipfile, internal, commit)
