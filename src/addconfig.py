#!/usr/bin/env python

import starexecparser
import telescope

def toAddConfig(te_creds, config_id, solver_id, name, description, commit):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        telescope.addConfig(dbcur, config_id, solver_id, name, description)
        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()


parser = starexecparser.StarExecParser('Post process a job.')
parser.addConfigId()
parser.addSolverId()
parser.addTelescopeCredentials()
parser.addOptionalName()
parser.addOptionalDescription()
parser.addCommit()


parser.processArgs()
te_creds    = parser.getTelescopeCredentials()
config_id   = parser.getConfigId()
solver_id   = parser.getSolverId()
name        = parser.getOptionalName()
description = parser.getOptionalDescription()
commit      = parser.getCommit()

toAddConfig(te_creds, config_id, solver_id, name, description, commit)
