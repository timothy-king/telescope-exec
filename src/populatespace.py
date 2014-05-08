#!/usr/bin/env python

import starexecparser
import starexecpipe
import telescope

def toPopulateSpace(te_creds, se_creds, space_id, space_name, commit, add_benchmarks, sep):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        with starexecpipe.StarExecPipe(se_creds, "toPopulateSpace") as pipe:
            telescope.populateSpace(pipe, dbcur, space_id, space_name, add_benchmarks, sep)

        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()


parser = starexecparser.StarExecParser('Populates a space on star exec recursively')
parser.addSpaceId()
parser.addSpaceName()
parser.addTelescopeCredentials()
parser.addStarExecCredentials()
parser.addOptionalName()
parser.addCommit()
parser.addAddBenchmarks()
parser.addSeperator()

parser.processArgs()
space_id       = parser.getSpaceId()
space_name     = parser.getSpaceName()
te_creds       = parser.getTelescopeCredentials()
se_creds       = parser.getStarExecCredentials()
commit         = parser.getCommit()
add_benchmarks = parser.getAddBenchmarks()
sep            = parser.getSeperator()


toPopulateSpace(te_creds, se_creds, space_id, space_name, commit, add_benchmarks, sep)
