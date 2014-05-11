
import starexecpipe
import telescope
import argparse

class StarExecParser:
    def __init__(self, desc):
        self.p = argparse.ArgumentParser(description=desc)
        self.args = None

    def getBaseParser(self):
        return self.p

    def processArgs(self):
        self.args = self.p.parse_args()

    def getArgs(self):
        return self.args

    def addTelescopeCredentials(self):
        self.p.add_argument('dbuser',
                            help="telescope mysql database username")
        self.p.add_argument('dbpassword',
                            help="telescope mysql database password")
        self.p.add_argument('--dbtable',
                            default="telescope",
                            help="telescope mysql database table")
        self.p.add_argument('--dbserver',
                            default="localhost",
                            help="telescope mysql database address")

    def getTelescopeCredentials(self):
        dbserver = self.args.dbserver
        dbuser = self.args.dbuser
        dbpassword = self.args.dbpassword
        dbtable = self.args.dbtable
        return telescope.TelescopeCredentials(dbserver, dbuser, dbpassword, dbtable)

    def addStarExecCredentials(self):
        self.p.add_argument('secommand', type=str,
                            help="location of StarexecCommand.jar")
        self.p.add_argument('seuser', type=str,
                            help="star exec username")
        self.p.add_argument('sepassword', type=str,
                            help="star star password")
        self.p.add_argument('--seserver',
                            default="https://www.starexec.org/starexec/",
                            help="address of the star exec server")

    def getStarExecCredentials(self):
        secommand = self.args.secommand
        seuser = self.args.seuser
        sepassword = self.args.sepassword
        seserver = self.args.seserver
        return starexecpipe.StarExecCredentials(secommand, seuser, sepassword, seserver)

    def addSeperator(self):
        self.p.add_argument('--sep', default="/",
                            help="path seperator string")
    def getSeperator(self):
        return self.args.sep

    def addCommit(self):
        self.p.add_argument('--commit', action='store_true',
                            help="commit the changes to the database")
    def getCommit(self):
        return self.args.commit

    def addSpaceId(self):
        self.p.add_argument('space_id', type=int, help="space id")
    def getSpaceId(self):
        return self.args.space_id

    def addSpaceName(self):
        self.p.add_argument('space_name', type=str, help="space name")
    def getSpaceName(self):
        return self.args.space_name

    def addConfigId(self):
        self.p.add_argument('config_id', type=int, help="configuration id")
    def getConfigId(self):
        return self.args.config_id

    def addSolverId(self):
        self.p.add_argument('solver_id', type=int, help="solver id")
    def getSolverId(self):
        return self.args.solver_id

    def addJobId(self):
        self.p.add_argument('job_id', type=int, help="job id")
    def getJobId(self):
        return self.args.job_id

    def addProblemSetId(self):
        self.p.add_argument('problem_set_id', type=int, help="problem set id")
    def getProblemSetId(self):
        return self.args.problem_set_id

    def addOptionalName(self):
        self.p.add_argument('--name', type=str, help="name")
    def getOptionalName(self):
        return self.args.name

    def addOptionalDescription(self):
        self.p.add_argument('--description', type=str, help="description")
    def getOptionalDescription(self):
        return self.args.description

    def addOptionalVersion(self):
        self.p.add_argument('--version', type=str, help="solver version")
    def getOptionalVersion(self):
        return self.args.version

    def addOptionalJobFields(self):
        self.p.add_argument('--cpu', type=float,
                            help="cpu time limit of the job")
        self.p.add_argument('--wc', type=float,
                            help="wall clock time limit of the job")
        self.p.add_argument('--mem', type=int,
                            help="memory limit of the job")
        self.p.add_argument('--email', type=str,
                            help="email of the person to the contacted for the job")
    def getOptionalJobFields(self):
        return (self.args.cpu, self.args.wc, self.args.mem, self.args.email)

    def addOptionalGenerateActiveJobPairs(self):
        self.p.add_argument('--generate_active_job_pairs',
                            action='store_true',
                            help="Populate the table with the active job pairs.")
    def getOptionalGenerateActiveJobPairs(self):
        return self.args.generate_active_job_pairs

    def addZipFile(self):
        self.p.add_argument('zipfile', type=str,
                            help="zip csv file. Guesses ./Job<jobid>_info.zip")
    def addOptionalZipFile(self):
        self.p.add_argument('--zipfile', type=str,
                            help="zip csv file. Guesses ./Job<jobid>_info.zip")
    def getZipFile(self):
        return self.args.zipfile

    def addOptionalInternalFile(self):
        self.p.add_argument('--internal', type=str,
                            help="csv file inside of the zip. Guesses ./Job<jobid>_info.csv")
    def getOptionalInternalFile(self):
        return self.args.internal

    def addOptionalNumWorkers(self, defval):
        self.p.add_argument('--workers', type=int, default=defval,
                            help="default number of workers")
    def getOptionalNumWorkers(self):
        return self.args.workers

    def addOptionalConfigIdList(self):
        self.p.add_argument('--config_ids', type=int, nargs='+',
                            help='A list of config ids')
    def getOptionalConfigIdList(self):
        return self.args.config_ids

    def addOptionalSpaceIdList(self):
        self.p.add_argument('--space_ids', type=int, nargs='+',
                            help='A list of space ids')
    def getOptionalSpaceIdList(self):
        return self.args.space_ids

    def addSpaceIdList(self):
        self.p.add_argument('space_ids', type=int, nargs='+',
                            help='A list of space ids')
    def getSpaceIdList(self):
        return self.args.space_ids


    def addAddBenchmarks(self):
        self.p.add_argument('--add_benchmarks',
                            action='store_true',
                            help='')
    def getAddBenchmarks(self):
        return self.args.add_benchmarks

    def addGenerateSpacePaths(self):
        self.p.add_argument('--generate_space_paths',
                            action='store_true',
                            help='')
    def getGenerateSpacePaths(self):
        return self.args.generate_space_paths

    def addPostprocessorChoices(self):
        group = self.p.add_mutually_exclusive_group(required=True)
        group.add_argument('--none', action='store_true')
        group.add_argument('--sat_result', action='store_true')
        group.add_argument('--cvc4_stats', action='store_true')
        group.add_argument('--glpk', action='store_true')
        group.add_argument('--scip', action='store_true')
    def getPostprocessorChoices(self):
        a = self.args
        return (a.none, a.sat_result, a.cvc4_stats, a.glpk, a.scip)

    def addAddNewStatistics(self):
        self.p.add_argument('--add_new_statistics',
                            action='store_true',
                            help='Add new statistics dynamically')
    def getAddNewStatistics(self):
        return self.args.add_new_statistics
