
from starexecpipe import *
import MySQLdb as mdb
import os
import csv
import tempfile
import zipfile
import wrappedprogressbar

class TelescopeCredentials:
    def __init__(self, dbserver, dbuser, dbpassword, dbtable):
        self.dbserver  = dbserver
        self.dbuser = dbuser
        self.dbpassword = dbpassword
        self.dbtable = dbtable
    def server(self):
        return self.dbserver
    def user(self):
        return self.dbuser
    def password(self):
        return self.dbpassword
    def table(self):
        return self.dbtable
    def __str__(self):
        s = 'TelescopeCredentials('
        s += repr(self.server())+', '
        s += repr(self.user())+', '
        s += repr(self.password())+', '
        s += repr(self.table())+')'
        return s

def connectUsingCredentials(creds):
    assert isinstance(creds, TelescopeCredentials)
    return mdb.connect(creds.server(), creds.user(), creds.password(), creds.table())


def statIntStr():
    return 'STAT_INT'
def statFloatStr():
    return 'STAT_FLOAT'
def statStringStr():
    return 'STAT_STR'

def classify(s):
    try:
        try:
            int(s)
            return statIntStr()
        except ValueError:
            float(s)
            return statFloatStr()
    except ValueError:
        return statStringStr()

class TelescopeResult:
    def __init__(self, result_id, job_config_pair_id, problem_set_to_benchmark_id, result=None, status=None, cpu_time=None, wallclock_time=None):
        self.result_id = result_id
        self.job_config_pair_id = job_config_pair_id
        self.problem_set_to_benchmark_id = problem_set_to_benchmark_id

        self.stats = dict()

        self.setResult(result)
        self.setStatus(status)
        self.setCpuTime(cpu_time)
        self.setWallclockTime(wallclock_time)

    def getResultId(self):
        return self.result_id

    def setStatus(self, status):
        self.status = status
        assert self.status is None or self.isComplete() or self.isTimeout() or self.isMemout(), status
    def clearStatus(self):
        self.status = None

    def getStatus(self):
        return self.status
    def setResult(self, result):
        self.result = result
        if self.result == "--":
            self.result = None
    def getResult(self):
        return self.result
    def setCpuTime(self, cpu_time):
        self.cpu_time = cpu_time
    def getCpuTime(self):
        return self.cpu_time
    def setWallclockTime(self, wallclock_time):
        self.wallclock_time = wallclock_time
    def getWallclockTime(self):
        return self.wallclock_time

    def setStat(self, key, value, classification=None):
        if classification is None:
            classification = classify(value)
        assert classification in [statIntStr(),  statFloatStr(), statStringStr()]
        self.stats[key] = (value, classification)

    def getStats(self):
        return self.stats

    def isComplete(self):
        return self.status == "complete"
    def isTimeout(self):
        return self.isCPUTimeout() or self.isWCTimeout()
    def isCPUTimeout(self):
        return self.status == "timeout (cpu)"
    def isWCTimeout(self):
        return self.status == "timeout (wallclock)"
    def isMemout(self):
        return self.status == "memout"


# def collectMatchResults(lines, pre_skips, proc, post_skips, matches):
#     all_matches = []
#     for line in lines:
#         if line in pre_skips:
#             continue
#         post = proc(line)
#         if post in post_skips:
#             continue

#         if post in matches:
#             all_matches.append(matches[post])
#     return all_matches

def accumulateMatchesStats(lines, ph0_skips, ph1, ph1_skips, ph1_errors, ph1_matches, ph2):
    all_matches = []
    all_stats = dict()
    for ph0_ln in lines:
        #print ph0_ln
        if ph0_ln in ph0_skips:
            continue
        ph1_ln = ph1(ph0_ln)
        if ph1_ln in ph1_skips:
            continue

        if ph1_ln in ph1_errors:
            all_matches.append('error')
            break

        if ph1_ln in ph1_matches:
            all_matches.append(ph1_matches[ph1_ln])
        else:
            ph2_res = ph2(ph1_ln)
            if ph2_res:
                #print ph2_res
                key,value = ph2_res
                all_stats[key] = value
    return all_matches, all_stats

def partitionOn(line, s):
    pre,part,post = line.partition(s)
    #print repr(time),repr(tab),repr(rem), repr(line)
    assert part == s
    return (pre.strip(), post.strip())

class ResultProcessor (object):
    def __init__(self):
        pass
    def apply(self, res, lines):
        """By default do nothing"""
        return False

class StarExecCollectMatches (ResultProcessor):
    def __init__(self, matches):
        self.ph0_skips = set(["\n"])
        self.ph1_skips = set(["EOF"])
        self.ph1_errors = set([])
        self.ph1_matches = matches
        self.ph2 = lambda x : False

    def apply(self, res, lines):
        pot = lambda x : (partitionOn(x,'\t'))[1]
        ph0_skips = self.ph0_skips
        ph1_skips = self.ph1_skips
        ph1_errors = self.ph1_errors
        ph1_matches = self.ph1_matches
        ph2 = self.ph2
        try:
            ms,ss = accumulateMatchesStats(lines, ph0_skips, pot, ph1_skips, ph1_errors, ph1_matches, ph2)
        except:
            print "exception during processing", res.getResultId()
            raise
        setSomething = False
        if len(ms) >= 1:
            first = ms[0]
            if all(map(lambda x: first == x, ms)):
                res.setResult(first)
                setSomething = True
            else:
                raise Exception("Multiple different results", ms)
        else:
            res.setResult("unknown")
            raise Exception, res.getResultId()

        for key, value in ss.iteritems():
            res.setStat(key, value)
            setSomething = True
        return setSomething


class GlpkCollectMatches (StarExecCollectMatches):
    def __init__(self):
        glpkmatches = dict([('PROBLEM HAS NO INTEGER FEASIBLE SOLUTION', 'unsat'),
                            ('PROBLEM HAS NO PRIMAL FEASIBLE SOLUTION', 'unsat'),
                            ('LP HAS NO PRIMAL FEASIBLE SOLUTION', 'unsat'),
                            ('INTEGER OPTIMAL SOLUTION FOUND', 'sat'),
                            ('OPTIMAL SOLUTION FOUND', 'sat')])
        super(GlpkCollectMatches,self).__init__(glpkmatches)

class ScipCollectMatches (StarExecCollectMatches):
    def __init__(self):
        scipmatches = dict([('SCIP Status        : problem is solved [infeasible]', 'unsat'),
                            ('SCIP Status        : problem is solved [optimal solution found]', 'sat'),
                            ('SCIP Error (-16): maximal branching depth level exceeded', 'unknown')])
        super(ScipCollectMatches,self).__init__(scipmatches)

class SatUnsatCollectMatches (StarExecCollectMatches):
    def __init__(self):
        satunsatmatches = dict([('unsat', 'unsat'),
                                ('sat', 'sat')])
        super(SatUnsatCollectMatches, self).__init__(satunsatmatches)

class CVC4StatsCollectMatches (SatUnsatCollectMatches):
    def __init__(self):
        super(CVC4StatsCollectMatches,self).__init__()
        self.ph2 = lambda x: partitionOn(x,', ')
        self.ph1_errors = set(['CVC4 suffered a segfault.',
                               'CVC4 threw an "unexpected" exception.'])


def getResult(dbcur, result_id, keep_result=None, keep_status=None, keep_cpu_time=None, keep_wallclock_time=None):
    rc=dbcur.execute(
              """SELECT result_id, job_config_pair_id, problem_set_to_benchmark_id,
                        result, status, cpu_time, wallclock_time
              FROM Results WHERE result_id=%s""", (result_id,))
    assert rc==1
    fet = dbcur.fetchone()
    res = TelescopeResult(fet[0], fet[1], fet[2])
    if keep_result:
        res.setResult(fet[3])
    if keep_status:
        res.setStatus(fet[4])
    if keep_cpu_time:
        res.setCpuTime(fet[5])
    if keep_wallclock_time:
        res.setWallclockTime(fet[6])
    return res

def writeResult(dbcur, res, add_stats):
    assert isinstance(res, TelescopeResult)
    result_id = res.getResultId()

    stats = res.getStats()
    result = res.getResult()
    status = res.getStatus()
    cpu_time = res.getCpuTime()
    wallclock_time = res.getWallclockTime()

    #Step 1)Sanity check that the result has not been processed
    rc=dbcur.execute(
              """SELECT bin(has_stats) FROM Results
              WHERE result_id=%s""", (result_id,))
    assert rc==1
    fet = dbcur.fetchone()
    #print fet
    assert fet[0] == '0', "Stats must not be set for this result"

    #print result_id, result

    #print 'result', result
    setOptionalArg(dbcur, 'Results', result_id, 'result_id', result, 'result')
    setOptionalArg(dbcur, 'Results', result_id, 'result_id', status, 'status')
    setOptionalArg(dbcur, 'Results', result_id, 'result_id', cpu_time, 'cpu_time')
    setOptionalArg(dbcur, 'Results', result_id, 'result_id', wallclock_time, 'wallclock_time')

    for name, (value, classification) in stats.iteritems():
        stat_id = safelyGetStatId(dbcur, name, classification, add_stats)
        writeStatResult(dbcur, result_id, stat_id, value, classification)

def safelyGetStatId(dbcur, name, classification, add_stats):
    stat_id = maybeSelectPrimaryKey(dbcur, 'Statistics', 'stat_id', 'name',  name)
    if stat_id is None:
        if add_stats:
            stat_id = addStatistic(dbcur, name, classification)
        else:
            raise Exception, ("Could not find statistic "+repr(name))
    else:
        stat_class = getValueFromPrimaryKey(dbcur, 'Statistics', 'stat_type', 'stat_id', stat_id)
        assert stat_class == classification, "Classification does not match"
    return stat_id

def addStatistic(dbcur, name, classificiation):
    rc=dbcur.execute("""INSERT INTO  Statistics (name, stat_type)
                     VALUES (%s,%s)""",
                     (name, classificiation ))
    checkRowCount(rc, 1)
    stat_id = maybeSelectPrimaryKey(dbcur, 'Statistics', 'stat_id', 'name',  name)
    assert stat_id is not None
    return stat_id

def writeStatResult(dbcur, result_id, stat_id, value, classification):
    if classification == statIntStr():
        rc=dbcur.execute("""INSERT INTO  StatisticResults (result_id, stat_id, int_value)
                     VALUES (%s,%s,%s)""",
                     (result_id, stat_id, int(value) ))
        checkRowCount(rc, 1)
    elif classification == statFloatStr():
        rc=dbcur.execute("""INSERT INTO  StatisticResults (result_id, stat_id, float_value)
                     VALUES (%s,%s,%s)""",
                     (result_id, stat_id, float(value) ))
        checkRowCount(rc, 1)

    elif classification == statStringStr():
        rc=dbcur.execute("""INSERT INTO  StatisticResults (result_id, stat_id, string_value)
                     VALUES (%s,%s,%s)""",
                     (result_id, stat_id, value ))
        checkRowCount(rc, 1)
    else:
        raise Exception('Bad classification string', classification)

def validIdentifier(s):
    f = lambda c: ( c.isalnum() or c == '_')
    return all(map(f, s))

def addSpace(dbcur, parent_space_id, space_id, space_name):
    if parent_space_id is None:
        rc=dbcur.execute("""INSERT INTO Spaces (space_id, name, active)
                      VALUES (%s,%s,%s)""",
                      (space_id, space_name, 1))
    else:
        rc=dbcur.execute("""INSERT INTO Spaces (space_id, parent_space_id, name, active)
                      VALUES (%s,%s,%s,%s)""",
                      (space_id, parent_space_id, space_name, 1))
    checkRowCount(rc, 1)

def addBenchmarks(dbcur, space_prefix, sep, benchmarks):
    bench_insert=[(bid, space_prefix+sep+bname, bname) for (bid,bname) in benchmarks]
    rc = dbcur.executemany(
        """INSERT INTO Benchmarks (bench_id, canonical_name, name)
        VALUES (%s,%s,%s)""", bench_insert)
    checkRowCount(rc, len(benchmarks))


def addBenchmarksToSpaces(dbcur, space_id, benchmarks):
    space_to_bench_insert=[(space_id, bid) for (bid,bname) in benchmarks]
    rc = dbcur.executemany(
        """INSERT INTO SpaceToBenchPairs (space_id, bench_id)
        VALUES (%s,%s)""", space_to_bench_insert)
    checkRowCount(rc, len(benchmarks))

def checkRowCount(rc, l):
    assert (rc is None and l == 0) or (rc == l), \
        "Expect row count %r to match #benchmarks %r" % (rc, l)


def populateSpace(pipe, dbcur, space_id, space_name, add_benchmarks, sep):
    """Populating space"""
    populateSpace_rec(pipe, dbcur, None, "", space_id, space_name, add_benchmarks, sep)

def populateSpace_rec(pipe, dbcur, parent_id, parent_space_prefix, space_id, space_name, add_benchmarks, sep):
    space_prefix = parent_space_prefix + sep + space_name
    print "opening", space_prefix, "(",space_id,")"

    addSpace(dbcur, parent_id, space_id, space_name)

    benchmarks = pipe.lsbenchmarks(space_id)
    if add_benchmarks:
        addBenchmarks(dbcur, space_prefix, sep, benchmarks)
    addBenchmarksToSpaces(dbcur, space_id, benchmarks)
    print "\t", "Added", len(benchmarks), "benchmarks"

    subspaces = pipe.lssubspaces(space_id)
    for (sid, name) in subspaces:
        populateSpace_rec(pipe, dbcur, space_id, space_prefix, sid, name, add_benchmarks, sep)
    print "exiting", space_prefix, "(",space_id,")"


def maybeSelectPrimaryKey(dbcur, table, primary_key_name, key_name,  key):
    assert validIdentifier(table)
    assert validIdentifier(primary_key_name)
    assert validIdentifier(key_name)

    sql_query='SELECT ' + primary_key_name +' FROM ' + table + ' WHERE '+ key_name +'=%s'
    rc=dbcur.execute(sql_query, (key,))
    assert rc <= 1
    if rc == 0:
        return None
    else:
        fet = dbcur.fetchone()
        return int(fet[0])

def getValueFromPrimaryKey(dbcur, table, id_name,  primary_key_name, primary_key):
    #print "getValueFromPrimaryKey","(",table, id_name,  primary_key_name, primary_key,")"

    assert validIdentifier(table)
    assert validIdentifier(id_name)
    assert validIdentifier(primary_key_name)
    sql_query='SELECT ' + id_name +' FROM ' + table + ' WHERE '+ primary_key_name+'=%s'
    rc=dbcur.execute(sql_query, (primary_key,))
    assert rc==1
    fet = dbcur.fetchone()
    #print "getValueFromPrimaryKey", fet
    assert len(fet) == 1
    return fet[0]

def getSpaceName(dbcur, space_id):
    return getValueFromPrimaryKey(dbcur, "Spaces", "name", "space_id", space_id)


def getBenchName(dbcur, bench_id):
    return getValueFromPrimaryKey(dbcur, "Benchmarks", "name", "bench_id", bench_id)

def getIdFromPrimaryKey(dbcur, table, id_name,  primary_key_name, primary_key):
    tmp=getValueFromPrimaryKey(dbcur, table, id_name,  primary_key_name, primary_key)
    return int(tmp)


def getProblemSetIdForJob(dbcur, job_id):
    return getIdFromPrimaryKey(dbcur, "Jobs", "problem_set_id", "job_id", job_id)

def getSpaceIdForProblemSet(dbcur, problem_set_id):
    return getIdFromPrimaryKey(dbcur, "ProblemSets", "space_id", "problem_set_id", problem_set_id)

def getProblemSetIdFromSpace(dbcur, space_id):
    return getIdFromPrimaryKey(dbcur, "ProblemSets", "problem_set_id", "space_id", space_id)

def getProblemSetToBenchmarkIdFromResult(dbcur, result_id):
    return getIdFromPrimaryKey(dbcur, "Results", "problem_set_to_benchmark_id", "result_id", result_id)

def getBenchIdFromProblemSetToBenchmarks(dbcur, problem_set_to_benchmark_id):
    return getIdFromPrimaryKey(dbcur, "ProblemSetToBenchmarks", "bench_id", "problem_set_to_benchmark_id",
                               problem_set_to_benchmark_id)



def getBenchIdFromResultId(dbcur, result_id):
    problem_set_to_benchmark_id = getProblemSetToBenchmarkIdFromResult(dbcur, result_id)
    return getBenchIdFromProblemSetToBenchmarks(dbcur, problem_set_to_benchmark_id)

def generateSpacePaths_rec(dbcur, sep, root_space_id, pre_path_string, space_id):
    space_name = getSpaceName(dbcur, space_id)
    path_string = pre_path_string + space_name
    print (root_space_id, space_id, path_string)
    rc=dbcur.execute("""INSERT INTO SpacePaths (start_space_id, end_space_id, path_string)
                     VALUES (%s,%s,%s)""",
                     (root_space_id, space_id, path_string))
    checkRowCount(rc, 1)

    dbcur.execute(
              """SELECT space_id FROM Spaces
              WHERE parent_space_id=%s""", (space_id,))
    children_fet = dbcur.fetchall()
    children = [x[0] for x in children_fet]
    children_path_string = path_string + sep
    for c in children:
        child_id = int(c)
        generateSpacePaths_rec(dbcur, sep, root_space_id, children_path_string, child_id)

def generateSpacePaths(dbcur, sep, root_space_id):
    generateSpacePaths_rec(dbcur, sep, root_space_id, "", root_space_id)
    dbcur.execute(
              """UPDATE Spaces SET path_closed=1
              WHERE space_id=%s""", (root_space_id,))

def setResultToHaveStats(dbcur, result_id):
    dbcur.execute(
              """UPDATE Results SET has_stats=1
              WHERE result_id=%s""", (result_id,))

def setOptionalArg(dbcur, table, key, key_name, arg, arg_name):
    if arg is not None:
        update_str='Update '+table+' SET '+arg_name+'=%s WHERE '+key_name+'=%s'
        rc = dbcur.execute(update_str, (arg, key))
        if rc != 0 and rc != 1:
            checkRowCount(rc, 1)

def addProblemSet(dbcur, ps_space_id, ps_name, ps_description):
    print "begin addProblemSet(", ps_space_id, repr(ps_name), repr(ps_description),")"

    #Step 1)Sanity check that space paths have been closed for ps_space_id
    rc=dbcur.execute(
              """SELECT bin(path_closed) FROM Spaces
              WHERE space_id=%s""", (ps_space_id,))
    assert rc==1
    fet = dbcur.fetchone()
    print fet
    assert fet[0] == '1', "Space paths must be closed before generating the problem set"

    #Step 2) insert the problem set
    rc = dbcur.execute(
         """INSERT INTO ProblemSets (space_id)
         VALUES (%s)""", (ps_space_id))
    checkRowCount(rc, 1)

    # get the just added id
    problem_set_id = getProblemSetIdFromSpace(dbcur, ps_space_id)

    setOptionalArg(dbcur, 'ProblemSets', problem_set_id, 'problem_set_id', ps_name, 'name')
    setOptionalArg(dbcur, 'ProblemSets', problem_set_id, 'problem_set_id', ps_description, 'description')


    #Step 3) get subspaces and space path ids
    rc = dbcur.execute(
               """SELECT space_path_id, end_space_id FROM SpacePaths
               WHERE start_space_id=%s""", (ps_space_id,))
    subspaces = dbcur.fetchall()

    print "adding benchmarks for", len(subspaces), "subspaces to space", ps_space_id

    #Step 4) For each subspace add benchmarks in the subspace to the problem set
    totalAdded = 0
    for (spid, ss_id) in subspaces:
        rc = dbcur.execute(
               """SELECT bench_id FROM SpaceToBenchPairs
               WHERE space_id=%s""", (ss_id,))
        bench_ids = dbcur.fetchall()
        ps2b_values = [ (problem_set_id, bid[0], spid) for bid in bench_ids]
        #print ps2b_values
        rc = dbcur.executemany(
            """INSERT INTO ProblemSetToBenchmarks (problem_set_id, bench_id, space_path_id)
               VALUES (%s,%s,%s)""", ps2b_values)
        rcint =  0 if rc is None else rc
        totalAdded += rcint
        assert rcint == len(ps2b_values)
    print "end addProblemSet(", ps_space_id, repr(ps_name), repr(ps_description),")",
    print "added", totalAdded

def addSolverToSpaceIds(dbcur, solver_id, space_ids):
    to_insert = [(solver_id, space_id) for space_id in space_ids]
    rc = dbcur.executemany(
            """INSERT INTO SolverToSpacePairs (solver_id, space_id)
               VALUES (%s,%s)""", to_insert)
    rcint =  0 if rc is None else rc
    assert rcint == len(space_ids)

def addSolverToSpaceIdPair(dbcur, solver_id, space_id):
    addSolverToSpaceIds(dbcur, solver_id, [space_id])

def addSolver(dbcur, solver_id, space_ids, name, description, version):
    rc = dbcur.execute(
        """INSERT INTO Solvers (solver_id)
        VALUES (%s)""", (solver_id))
    checkRowCount(rc, 1)

    setOptionalArg(dbcur, 'Solvers', solver_id, 'solver_id', name, 'name')
    setOptionalArg(dbcur, 'Solvers', solver_id, 'solver_id', description, 'description')
    setOptionalArg(dbcur, 'Solvers', solver_id, 'solver_id', version, 'version')

    if space_ids:
        addSolverToSpaceIds(dbcur, solver_id, space_ids)

    print "addSolver(", solver_id,",", space_ids,",", name,",", description,",", version,")"


def addConfig(dbcur, config_id, solver_id, name, description):
    rc = dbcur.execute(
        """INSERT INTO Configurations (config_id, solver_id)
        VALUES (%s,%s)""", (config_id, solver_id))
    checkRowCount(rc, 1)

    setOptionalArg(dbcur, 'Configurations', config_id, 'config_id', name, 'name')
    setOptionalArg(dbcur, 'Configurations', config_id, 'config_id', description, 'description')

    print "addConfig(", solver_id,",",config_id,",", name,",", description,")"

def addJob(dbcur, job_id, problem_set_id, name, description, cpu, wc, mem, email):
    rc = dbcur.execute(
        """INSERT INTO Jobs (job_id, problem_set_id)
        VALUES (%s,%s)""", (job_id, problem_set_id))
    checkRowCount(rc, 1)

    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', name, 'name')
    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', description, 'description')
    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', cpu, 'cpu_time_limit')
    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', wc, 'wc_time_limit')
    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', mem, 'memory_limit')
    setOptionalArg(dbcur, 'Jobs', job_id, 'job_id', email, 'email')

    print "addJob(", job_id, problem_set_id, name, description, cpu, wc, mem, email,")"

def getActiveJobConfigs(dbcur, job_id):
    """ Add non-recycled configurations to a job"""
    problem_set_id = getProblemSetIdForJob(dbcur, job_id)
    space_id = getSpaceIdForProblemSet(dbcur, problem_set_id)

    rc=dbcur.execute(
        """SELECT %s, Configurations.config_id
         FROM SolverToSpacePairs
         inner join Solvers
           ON SolverToSpacePairs.solver_id=Solvers.solver_id
         inner join Configurations
           ON Solvers.solver_id=Configurations.solver_id
         WHERE SolverToSpacePairs.space_id=%s
           and bin(Solvers.recycled)=0
           and bin(Configurations.recycled)=0""",
        (job_id, space_id))
    non_recycled_job_config_pairs = dbcur.fetchall()
    print non_recycled_job_config_pairs
    return non_recycled_job_config_pairs

def insertJobConfigPairs(dbcur, job_config_pairs):
    rc=dbcur.executemany(
        """INSERT INTO JobConfigurationPairs (job_id, config_id)
           VALUES (%s,%s)""", job_config_pairs)
    rcint =  0 if rc is None else rc
    assert rcint == len(job_config_pairs)
    print "Generated Job configuration pairs", rcint

def addJobConfigPairs(dbcur, job_id):
    """ Add all of the non-recycled jobs"""
    non_recycled_job_config_pairs = getActiveJobConfigs(dbcur, job_id)
    insertJobConfigPairs(dbcur, non_recycled_job_config_pairs)


def addSubsetJobConfigPairs(dbcur, job_id, config_ids):
    """ Add non-recycled configurations to a job"""
    cids = set(config_ids)
    jcps = getActiveJobConfigs(dbcur, job_id)
    func = lambda (j,c): c in cids
    rem = filter(lambda (j,c): c in cids, jcps)
    print rem, config_ids
    assert len(rem) == len(config_ids)
    insertJobConfigPairs(dbcur, rem)



def parseJobInfoCSV(csv_file):
    """Parse CSV file corresponding to job and collect results.
    TODO: sat/unsat result currently corrected from log file
    because starexec postprocessor does not work with stats.
    """
    num_lines = numLinesInFile(csv_file)
    bar = wrappedprogressbar.WrappedProgressBar(num_lines - 1)
    rows = []
    with open(csv_file, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            bar.increment()

            #pair_id, benchmark id, solver id, configuration id, status,cpu time,wallclock time,result
            pair_id = int(row['pair id'])
            bench_id = int(row['benchmark id'])
            solver_id = int(row['solver id'])
            config_id = int(row['configuration id'])
            cpu_time = float(row['cpu time'])
            wall_time = float(row['wallclock time'])
            status = row['status']
            result = row['result']
            assert result=="--", "TODO: Time to collect result from .csv"
            #pair_id, benchmark id, solver id, configuration id, status,cpu time,wallclock time,result
            res=(pair_id, bench_id, solver_id, config_id, status, cpu_time, wall_time,result)
            rows.append(res)
        bar.finish()
    print num_lines, len(rows)
    assert num_lines == len(rows)+1
    return rows

def addResultRows(dbcur, job_id, rows):
    #[(pair_id, bench_id, solver_id, config_id, status, cpu_time, wall_time,result)]
    # pair_id is in 1-1 correspondence with Results.result_id
    # need to get job_config_ids, problem_set_to_benchmark_ids


    # for each configuration get the job configuration id
    jc_ids = dict()
    for (pid, bid, sid, cid, status, cpu, wall, res) in rows:
        if cid not in jc_ids:
            rc=dbcur.execute(
                """SELECT job_config_pair_id FROM JobConfigurationPairs
                   WHERE job_id=%s AND config_id=%s""", (job_id,cid))
            print job_id, cid
            fet = dbcur.fetchone()
            print rc, fet
            assert rc==1
            jc_ids[cid] = int(fet[0])

    problem_set_id = getProblemSetIdForJob(dbcur, job_id)

    # for each benchmark get the problem set benchmark id
    problem_set_to_benchmark_ids = dict()
    for (pid, bid, sid, cid, status, cpu, wall, res) in rows:
        if bid not in problem_set_to_benchmark_ids:
            rc=dbcur.execute(
            """SELECT problem_set_to_benchmark_id FROM ProblemSetToBenchmarks
               WHERE problem_set_id=%s AND bench_id=%s""", (problem_set_id, bid))
            assert rc==1
            fet = dbcur.fetchone()
            problem_set_to_benchmark_ids[bid] = int(fet[0])

    # reformat rows for the insertion operation
    result_to_insert = [(pid, jc_ids[cid], problem_set_to_benchmark_ids[bid], res, status, cpu, wall)
                        for (pid, bid, sid, cid, status, cpu, wall, res) in rows]


    rc=dbcur.executemany(
        """INSERT INTO Results
          (result_id, job_config_pair_id, problem_set_to_benchmark_id, result, status, cpu_time, wallclock_time)
          VALUES (%s, %s, %s, %s, %s, %s, %s)""", result_to_insert)
    rcint =  0 if rc is None else rc
    assert rcint == len(result_to_insert)

def addCsvFile(dbcur, job_id, csv_file):
    if csv_file is None:
        csv_file = 'Job'+str(job_id)+'_info.csv'
    rows = parseJobInfoCSV(csv_file)
    addResultRows(dbcur, job_id, rows)

def addZippedCsvFile(dbcur, job_id, zip_file, internal_file):
    if zip_file is None:
        zip_file = 'Job'+str(job_id)+'_info.zip'
    if internal_file is None:
        internal_file = 'Job'+str(job_id)+'_info.csv'
    with zipfile.ZipFile(zip_file, 'r') as zf:
        with tempfile.NamedTemporaryFile(mode='r+', delete=True) as tmp:
            tmp.write(zf.read(internal_file))
            tmp.flush()
            addCsvFile(dbcur, job_id, tmp.name)

def postProcessResult(pipe, dbcur, result_id):
    problem_set_to_benchmark_id = getProblemSetToBenchmarkIdFromResult(dbcur, result_id)
    bench_id = getBenchIdFromProblemSetToBenchmarks(dbcur, problem_set_to_benchmark_id)
    name = getBenchName(dbcur, bench_id)

    with tempfile.NamedTemporaryFile(mode='r+', suffix='.zip', delete=True) as tmpzip:
        pipe.getjobpair(result_id, tmpzip.name, True)
        tmpzip.flush()
        with zipfile.ZipFile(tmpzip.name, 'r') as zf:
            with tempfile.NamedTemporaryFile(mode='r+', delete=True) as tmpextract:
                tmpextract.write(zf.read(name))
                tmpextract.flush()

                print subprocess.check_output(['cat', tmpextract.name])


def postprocessResults(pipe, dbcur, job_id):
    rc=dbcur.execute(
            """SELECT Results.result_id
               FROM JobConfigurationPairs INNER JOIN Results
               ON JobConfigurationPairs.job_config_pair_id = Results.job_config_pair_id
               WHERE JobConfigurationPairs.job_id=%s and bin(Results.has_stats)=0 """,
            (job_id,))
    result_ids = dbcur.fetchall()
    assert len(result_ids) == rc
    bar = wrappedprogressbar.WrappedProgressBar(rc)
    processed = 0
    for (result_id,) in result_ids:
        postProcessResult(pipe, dbcur, result_id)
        processed += 1
        bar.increment()
    print "postprocessResults added", rc,"more benchmarks"


def toPostprocessResultsForJob(dbcur, job_id):
    rc=dbcur.execute(
        """SELECT Results.result_id
           FROM JobConfigurationPairs INNER JOIN Results
           ON JobConfigurationPairs.job_config_pair_id = Results.job_config_pair_id
           WHERE JobConfigurationPairs.job_id=%s and bin(Results.has_stats)=0 """,
        (job_id,))
    result_ids = dbcur.fetchall()
    assert len(result_ids) == rc
    return [rid for (rid,) in result_ids]


# subparsers = parser.add_subparsers(help='subcommand help')

# ###########################################
# ### parser populate
# ###########################################
# parser_pop = subparsers.add_parser('populate',
#                                    help='populate database from star exec. A RARE operation!')
# parser_pop.add_argument('pop_spaceid', type=int,
#                         help="space id to populate from")
# parser_pop.add_argument('spacename', type=str,
#                         help="space name to populate from")
# parser_pop.add_argument('secommand', type=str,
#                         help="location of StarexecCommand.jar")
# parser_pop.add_argument('seuser', type=str,
#                        help="star exec username")
# parser_pop.add_argument('sepassword', type=str,
#                         help="star star password")
# parser_pop.add_argument('--seserver',
#                         default="https://www.starexec.org/starexec/",
#                         help="address of the star exec server")


# parser_fill_in = subparsers.add_parser('fill-in',
#                                        help='fill in a database from star exec.!')
# parser_fill_in.add_argument('fill_spaceid', type=int,
#                             help="space id to fill in from")
# parser_fill_in.add_argument('spacename', type=str,
#                             help="space name to populate from")
# parser_fill_in.add_argument('secommand', type=str,
#                             help="location of StarexecCommand.jar")
# parser_fill_in.add_argument('seuser', type=str,
#                             help="star exec username")
# parser_fill_in.add_argument('sepassword', type=str,
#                             help="star star password")
# parser_fill_in.add_argument('--seserver',
#                             default="https://www.starexec.org/starexec/",
#                             help="address of the star exec server")


# ###########################################
# ### generate space paths
# ###########################################
# parser_gsp = subparsers.add_parser('generate-space-paths',
#                                    help="""Generates a paths for a space.
#                                            A precursor to making problems sets""")
# parser_gsp.add_argument('gsp_spaceid', type=int,
#                         help="root space id to generate paths from")


# ###########################################
# ### Database addition commands
# ###########################################
# parser_aps = subparsers.add_parser('dbadd-problem-set',
#                                     help="""Adds a problem set to the database""")
# parser_aps.add_argument('aps_spaceid', type=int,
#                         help="root space id to generate paths from")
# parser_aps.add_argument('--name', type=str,
#                         help="problem set name")
# parser_aps.add_argument('--description', type=str,
#                         help="problem set description")


# parser_asolver = subparsers.add_parser('dbadd-solver',
#                                         help="""Adds a PRE-EXISTING solver to the database""")
# parser_asolver.add_argument('asolver_spaceid', type=int,
#                             help="space id the solver is in")
# parser_asolver.add_argument('solver_id', type=int,
#                             help="solver id")
# parser_asolver.add_argument('--name', type=str,
#                             help="solver name")
# parser_asolver.add_argument('--version', type=str,
#                             help="solver version")
# parser_asolver.add_argument('--description', type=str,
#                             help="solver description")


# parser_gajcp = subparsers.add_parser(
#     'generate_active_job_pairs',
#     help="""Populate the table with the configurations for active solvers in the space.""")
# parser_gajcp.add_argument('gen_active_job_id', type=int,
#                          help="job id")


# parser_azipcsv = subparsers.add_parser(
#     'dbadd-zip-results', help="""Adds a star exec job info csv file into the results table""")
# parser_azipcsv.add_argument('add_zip_job_id', type=int,
#                             help="job id")
# parser_azipcsv.add_argument('--zip', type=str,
#                             help="zip csv file. Guesses ./Job<jobid>_info.zip")
# parser_azipcsv.add_argument('--internal', type=str,
#                             help="csv file inside of the zip. Guesses ./Job<jobid>_info.csv")


# args = parser.parse_args()
# print "arguments", args
# print
# print




# dbcon = mdb.connect(args.dbserver, args.dbuser, args.dbpassword, args.dbtable)
# with dbcon:
#     dbcur = dbcon.cursor()

# handled
#     if hasattr(args, 'pop_spaceid'):
#         with StarExecPipe(args.secommand, args.seuser, args.sepassword, args.seserver) as pipe:
#             populateSpace(pipe, dbcur, args.pop_spaceid, arsg.spacename, args.sep)
#     if hasattr(args, 'fill_spaceid'):
#         with StarExecPipe(args.secommand, args.seuser, args.sepassword, args.seserver) as pipe:
#             fillInSpace(pipe, dbcur, args.fill_spaceid, args.spacename)

#     elif hasattr(args, 'gsp_spaceid'):
#         generateSpacePaths(dbcur, args.sep, args.gsp_spaceid)
#     elif hasattr(args, 'aps_spaceid'):
#         addProblemSet(dbcur, args.aps_spaceid, args.name, args.description)

#     elif hasattr(args, 'asolver_spaceid'):
#         addSolver(dbcur, args.solver_id, args.asolver_spaceid,
#                   args.name, args.description, args.version)
#     elif hasattr(args, 'aconfig_id'):
#         addConfig(dbcur, args.aconfig_id, args.solver_id,
#                   args.name, args.description)
#     elif hasattr(args, 'add_job_id'):
#         addJob(dbcur, args.add_job_id, args.problem_set_id,
#                args.name, args.description, args.cpu, args.wc, args.mem,
#                args.email)
#         if args.generate_active_job_pairs:
#             generateActiveJobPairs(dbcur, args.add_job_id)
#     elif hasattr(args, 'gen_active_job_id'):
#         generateActiveJobPairs(dbcur, args.gen_active_job_id)
#     elif hasattr(args, 'add_zip_job_id'):
#         addZippedCsvFile(dbcur, args.add_zip_job_id, args.zip, args.internal)
#     elif hasattr(args, 'postprocess_job_id'):
#         with StarExecPipe(args.secommand, args.seuser, args.sepassword, args.seserver) as pipe:
#             postprocessResults(pipe, dbcur, args.postprocess_job_id)

#     if args.commit:
#         dbcon.commit()
#     else:
#         dbcon.rollback()
# dbcon.close()
