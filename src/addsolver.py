#!/usr/bin/env python

import starexecparser
import telescope


def toAddSolver(te_creds, solver_id, space_ids, name, description, version, commit):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        telescope.addSolver(dbcur, solver_id, space_ids, name, description, version)

        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()


parser = starexecparser.StarExecParser('Add a solver.')
parser.addSolverId()
parser.addTelescopeCredentials()
parser.addOptionalName()
parser.addOptionalDescription()
parser.addOptionalVersion()
parser.addCommit()
parser.addOptionalSpaceIdList()


parser.processArgs()
te_creds    = parser.getTelescopeCredentials()
solver_id   = parser.getSolverId()
name        = parser.getOptionalName()
description = parser.getOptionalDescription()
version     = parser.getOptionalVersion()
commit      = parser.getCommit()
space_ids   = parser.getOptionalSpaceIdList()

toAddSolver(te_creds, solver_id, space_ids, name, description, version, commit)
