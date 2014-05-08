#!/usr/bin/env python

import starexecparser
import telescope

def toAddJob(te_creds, job_id, problem_set_id, name, description, job_fields, commit, config_ids):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        cpu, wc, mem, email = job_fields
        telescope.addJob(dbcur, job_id, problem_set_id, name, description, cpu, wc, mem, email)
        if gajps:
            telescope.addJobConfigPairs(dbcur, job_id)
        print config_ids
        if config_ids:
            telescope.addSubsetJobConfigPairs(dbcur, job_id, config_ids)
        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()


parser = starexecparser.StarExecParser('Adds a PRE-EXISTING job to the database for an existing problem set')
parser.addJobId()
parser.addProblemSetId()
parser.addTelescopeCredentials()
parser.addOptionalName()
parser.addOptionalDescription()
parser.addOptionalJobFields()
parser.addOptionalGenerateActiveJobPairs()
parser.addOptionalConfigIdList()
parser.addCommit()


parser.processArgs()
te_creds       = parser.getTelescopeCredentials()
job_id         = parser.getJobId()
problem_set_id = parser.getProblemSetId()
name           = parser.getOptionalName()
description    = parser.getOptionalDescription()
job_fields     = parser.getOptionalJobFields()
commit         = parser.getCommit()
gajps          = parser.getOptionalGenerateActiveJobPairs()
config_ids     = parser.getOptionalConfigIdList()

toAddJob(te_creds, job_id, problem_set_id, name, description, job_fields, commit, config_ids)
