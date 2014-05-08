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


worker_threw_exception = threading.Event()

class WorkerThread(threading.Thread):
    def __init__(self, te_creds, se_creds, q, tid, commit, bar, total):
        super(WorkerThread, self).__init__()
        self.te_creds = te_creds
        self.se_creds = se_creds
        self.q = q
        self.tid = tid
        self.commit = commit
        self.bar = bar
        self.total = total
        self.stoprequest = threading.Event()

    def run(self):
        prefix = 'Thread '+str(self.tid)
        dbcon = telescope.connectUsingCredentials(self.te_creds)
        try:
            dbcur = dbcon.cursor()
            with starexecpipe.StarExecPipe(self.se_creds, prefix) as pipe:
                with tempfile.NamedTemporaryFile(mode='r+', suffix='.zip', delete=True) as tmpzip:
                    with tempfile.NamedTemporaryFile(mode='r+', delete=True) as tmpextract:
                        while (not self.stoprequest.isSet()) and (not worker_threw_exception.isSet()):
                            result_id = self.q.get(False)

                            self.do_work(dbcur, pipe, result_id, tmpzip, tmpextract)
                            if self.commit:
                                dbcon.commit()
                            else:
                                dbcon.rollback()
                            self.q.task_done()
                            self.bar.increment()
        except Queue.Empty:
            pass
        except Exception, e:
            worker_threw_exception.set()
            raise
        finally:
            dbcon.close()
        print "exiting", self.tid

    def join(self, timeout=None):
        self.stoprequest.set()
        super(WorkerThread, self).join(timeout)

    def do_work(self, dbcur, pipe, result_id, tmpzip, tmpextract):
        problem_set_to_benchmark_id = telescope.getProblemSetToBenchmarkIdFromResult(dbcur, result_id)
        bench_id = telescope.getBenchIdFromProblemSetToBenchmarks(dbcur, problem_set_to_benchmark_id)
        name = telescope.getBenchName(dbcur, bench_id)

        pipe.getjobpair(result_id, tmpzip.name, True)
        tmpzip.flush()

        with zipfile.ZipFile(tmpzip.name, 'r') as zf:
            #tmpextract.write(zf.read(name))
            #tmpextract.flush()

            collectStatisticsFromFile(dbcur, result_id, zf.open(name, 'r'))

            telescope.setResultToHaveStats(dbcur, result_id)


def classify(s):
    try:
        try:
            int(s)
            return 'STAT_INT'
        except ValueError:
            float(s)
            return 'STAT_FLOAT'
    except ValueError:
        return 'STAT_STR'

def collectStatisticsFromFile(dbcur, result_id, zf):
    """Collect the statistics from a given log file.

    Keyword arguments:
    path -- log file path
    result_id -- database id in JobResults corresponding to job and problem
    """
    skips = set(['EOF', 'sat', 'unsat'])
    stats = dict()
    for line in zf:
        # find tab
        if line == "\n":
            continue # skip blank lines
        time,tab,rem = line.partition('\t')
        #print repr(time),repr(tab),repr(rem), repr(line)
        assert tab=='\t'

        key, sep, value = rem.partition(', ')
        #print repr(key), repr(sep), repr(value)
        if sep == "":
            if key:
                continue
            else:
                print repr(key)
                raise Exception("malformed statisitics string:"+repr(line))
        assert sep==', '
        tr = str.strip(value)
        stats[key] = (tr, classify(tr))
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

def spawnWorkers(result_ids, num_workers, te_creds, se_creds, commit):
    assert num_workers > 0
    bar = wrappedprogressbar.WrappedProgressBar(len(result_ids))

    q = Queue.Queue()
    for i in range(num_workers):
        worker_args = (te_creds, se_creds, q, i, commit, bar, len(result_ids))
        t = WorkerThread(te_creds, se_creds, q, i, commit, bar, len(result_ids))
        t.start()

    for result_id in result_ids:
        q.put(result_id)

    q.join()


def toPostprocessResults(te_creds, job_id):
    dbcon = telescope.connectUsingCredentials(te_creds)
    with dbcon:
        dbcur = dbcon.cursor()
        return telescope.toPostprocessResultsForJob(dbcur, job_id)
    dbcon.close()

def postProcessJob(te_creds, se_creds, job_id, num_workers, commit):
    result_ids = toPostprocessResults(te_creds, job_id)
    print "Results to post process for", job_id
    print "\t", result_ids
    spawnWorkers(result_ids, num_workers, te_creds, se_creds, commit)


parser = starexecparser.StarExecParser('Post process a job.')
parser.addJobId()
parser.addTelescopeCredentials()
parser.addStarExecCredentials()
parser.addOptionalNumWorkers(5)
parser.addCommit()

parser.processArgs()
te_creds    = parser.getTelescopeCredentials()
se_creds    = parser.getStarExecCredentials()
job_id      = parser.getJobId()
num_workers = parser.getOptionalNumWorkers()
commit      = parser.getCommit()


print "no processing", time.clock()
postProcessJob(te_creds, se_creds, job_id, num_workers, commit)

print "done", time.clock()
