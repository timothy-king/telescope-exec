#!/usr/bin/env python

import starexecparser
import telescope

def toAddProblemSet(te_creds, space_id,  name, description, sep, generate_space_paths, commit):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()

        if generate_space_paths:
             telescope.generateSpacePaths(dbcur, sep, space_id)

        telescope.addProblemSet(dbcur, space_id,  name, description)

        if commit:
            dbcon.commit()
        else:
            dbcon.rollback()
    dbcon.close()


parser = starexecparser.StarExecParser('Adds a problem set')
parser.addSpaceId()
parser.addTelescopeCredentials()
parser.addOptionalName()
parser.addOptionalDescription()
parser.addGenerateSpacePaths()
parser.addSeperator()
parser.addCommit()


parser.processArgs()
te_creds       = parser.getTelescopeCredentials()
space_id       = parser.getSpaceId()
name           = parser.getOptionalName()
description    = parser.getOptionalDescription()
sep            = parser.getSeperator()
commit         = parser.getCommit()
generate_space_paths = parser.getGenerateSpacePaths()

toAddProblemSet(te_creds, space_id,  name, description, sep,  generate_space_paths, commit)
