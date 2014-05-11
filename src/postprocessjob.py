#!/usr/bin/env python

import starexecparser
import starexecpipe
import telescope
import wrappedprogressbar
import tempfile
import zipfile
import Queue
import threading
import subprocess
import time


shutdown_exception = threading.Event()

class WorkerThread(threading.Thread):
    def __init__(self, te_creds, se_creds, q, tid, commit, bar, total, ppchoices, add_new_stats):
        super(WorkerThread, self).__init__()
        self.te_creds = te_creds
        self.se_creds = se_creds
        self.q = q
        self.tid = tid
        self.commit = commit
        self.bar = bar
        self.total = total
        # ppchoices :: (a.none, a.sat_result, a.cvc4_stats, a.glpk, a.scip)
        if ppchoices[0]:
            self.rp = telescope.ResultProcessor()
        elif ppchoices[1]:
            self.rp = telescope.SatUnsatCollectMatches()
        elif ppchoices[2]:
            self.rp = telescope.CVC4StatsCollectMatches()
        elif ppchoices[3]:
            self.rp = telescope.GlpkCollectMatches()
        elif ppchoices[4]:
            self.rp = telescope.ScipCollectMatches()
        else:
            raise Exception("invalid pre processor choices", ppchoices)
        self.add_new_stats = add_new_stats
        self.done = threading.Event()


    def run(self):
        prefix = 'Thread '+str(self.tid)
        dbcon = telescope.connectUsingCredentials(self.te_creds)
        try:
            dbcur = dbcon.cursor()
            with starexecpipe.StarExecPipe(self.se_creds, prefix) as pipe:
                with tempfile.NamedTemporaryFile(mode='r+', suffix='.zip', delete=True) as tmpzip:
                    while not shutdown_exception.isSet():
                        result_id = self.q.get(False)

                        self.do_work(dbcur, pipe, result_id, tmpzip)
                        if self.commit:
                            dbcon.commit()
                        else:
                            dbcon.rollback()
                        self.q.task_done()
                        self.bar.increment()
        except Queue.Empty:
            pass
        except (KeyboardInterrupt, SystemExit):
            shutdown_exception.set()
        except Exception, e:
            shutdown_exception.set()
            raise
        finally:
            dbcon.close()
            self.done.set()
        print "exiting", self.tid

    def isdone(self):
        return self.done.isSet()


    def do_work(self, dbcur, pipe, result_id, tmpzip):
        result = telescope.getResult(dbcur, result_id, keep_status=True)

        bench_id = telescope.getBenchIdFromResultId(dbcur, result_id)
        name = telescope.getBenchName(dbcur, bench_id)


        if result.isComplete():
            pipe.getjobpair(result_id, tmpzip.name, True)
            tmpzip.flush()

            with zipfile.ZipFile(tmpzip.name, 'r') as zf:
                with zf.open(name, 'r') as internal:
                    modifications = self.rp.apply(result,  internal)
                    if modifications:
                        result.clearStatus()
                        telescope.writeResult(dbcur, result, self.add_new_stats)

# def classify(s):
#     try:
#         try:
#             int(s)
#             return 'STAT_INT'
#         except ValueError:
#             float(s)
#             return 'STAT_FLOAT'
#     except ValueError:
#         return 'STAT_STR'

# def collectStatisticsFromFile(dbcur, result_id, zf):
#     """Collect the statistics from a given log file.

#     Keyword arguments:
#     path -- log file path
#     result_id -- database id in JobResults corresponding to job and problem
#     """
#     skips = set(['EOF', 'sat', 'unsat'])
#     stats = dict()
#     for line in zf:
#         # find tab
#         if line == "\n":
#             continue # skip blank lines
#         time,tab,rem = line.partition('\t')
#         #print repr(time),repr(tab),repr(rem), repr(line)
#         assert tab=='\t'

#         key, sep, value = rem.partition(', ')
#         #print repr(key), repr(sep), repr(value)
#         if sep == "":
#             if key:
#                 continue
#             else:
#                 print repr(key)
#                 raise Exception("malformed statisitics string:"+repr(line))
#         assert sep==', '
#         tr = str.strip(value)
#         stats[key] = (tr, classify(tr))
    #print stats


            # for stat in stats:
            #     index = line.find(stat.name)
            #     if index < 0:
            #         continue
            #     stat_str = line[index:]
            #     tokens = stat_str.split(',')
            #     stat_name = tokens[0]
            #     stat_value = tokens[1]
            #     stat_res = StatResult(stat.id, stat.type, result_id, value)
            #     self.stat_results.append(stat_res)


# def worker(te_creds, se_creds, q, tid, commit, bar, total):
#     dbcon = telescope.connectUsingCredentials(te_creds)
#     try:
#         dbcur = dbcon.cursor()
#         with starexecpipe.StarExecPipe(se_creds) as pipe:
#             with tempfile.NamedTemporaryFile(mode='r+', suffix='.zip', delete=True) as tmpzip:
#                 with tempfile.NamedTemporaryFile(mode='r+', delete=True) as tmpextract:
#                     while not worker_through_exception.isSet():
#                         result_id = q.get()
#                         do_work(dbcur, pipe, result_id, tmpzip, tmpextract)
#                         if commit:
#                             dbcon.commit()
#                         else:
#                             dbcon.rollback()
#                         q.task_done()
#                         bar.increment()
#     except Queue.Empty:
#         pass
#     except Exception, e:
#         worker_through_exception.set()
#         raise
#     finally:
#         dbcon.close()

def spawnWorkers(result_ids, num_workers, te_creds, se_creds, ppchoices, add_new_stats, commit):
    assert num_workers > 0
    bar = wrappedprogressbar.WrappedProgressBar(len(result_ids))
    q = Queue.Queue()

    try:
        workers = []
        for i in range(num_workers):
            worker_args = (te_creds, se_creds, q, i, commit, bar, len(result_ids), ppchoices, add_new_stats,)
            t = WorkerThread(te_creds, se_creds, q, i, commit, bar, len(result_ids), ppchoices, add_new_stats)
            t.start()
            workers.append(t)


        for result_id in result_ids:
            q.put(result_id)

        while True:
            time.sleep(1)
            if all(map(lambda x: x.isdone(), workers)):
                return

    except KeyboardInterrupt:
        shutdown_exception.set()
        print "shutting down"
        time.sleep(5)

def toPostprocessResults(te_creds, job_id):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        return telescope.toPostprocessResultsForJob(dbcur, job_id)
    dbcon.close()

def postProcessJob(te_creds, se_creds, job_id, num_workers, ppchoices, add_new_stats, commit):
    result_ids = toPostprocessResults(te_creds, job_id)
    print "Results to post process for", job_id
    print "\t", result_ids
    spawnWorkers(result_ids, num_workers, te_creds, se_creds, ppchoices, add_new_stats, commit)


parser = starexecparser.StarExecParser('Post process a job.')
parser.addJobId()
parser.addTelescopeCredentials()
parser.addStarExecCredentials()
parser.addOptionalNumWorkers(5)
parser.addCommit()
parser.addAddNewStatistics()
parser.addPostprocessorChoices()


parser.processArgs()
te_creds     = parser.getTelescopeCredentials()
se_creds     = parser.getStarExecCredentials()
job_id       = parser.getJobId()
num_workers  = parser.getOptionalNumWorkers()
commit       = parser.getCommit()
add_new_stats= parser.getAddNewStatistics()
ppchoices    = parser.getPostprocessorChoices()

assert sum(ppchoices) == 1

if add_new_stats:
    assert num_workers == 1, "Can only add statistics with one worker thread"

print "no processing", time.clock()
postProcessJob(te_creds, se_creds, job_id, num_workers, ppchoices, add_new_stats, commit)

print "done", time.clock()
