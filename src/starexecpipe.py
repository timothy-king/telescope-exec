import subprocess

def numLinesInFile(fname):
    """ Counts the number of lines in the given file.
    """
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1

def lineIntoMap(ln):
    """id=1715 : name=March 7 2013\n"""
    pairs = ln.split(':')
    m = dict()
    for p in pairs:
        (key,eq,val) = p.partition('=')
        assert eq != ''
        m[key.strip()] = val.strip()
    return m

def idnameMapIntoPairs(m):
    assert 'id' in m
    assert 'name' in m
    return (int(m['id']), m['name'])

class StarExecCredentials:
    def __init__(self, starexeccommand, username, password, addr):
        self.secommand = starexeccommand
        self.seusername = username
        self.sepassword = password
        self.seaddr = addr

    def starexeccommand(self):
        return self.secommand
    def username(self):
        return self.seusername
    def password(self):
        return self.sepassword
    def address(self):
        return self.seaddr
    def __str__(self):
        s = 'StarExecCredentials('
        s += repr(self.starexeccommand())+', '
        s += repr(self.username())+', '
        s += repr(self.password())+', '
        s += repr(self.address())+')'
        return s

class StarExecPipe:
    """A pipe to execute a sequence of star exec commands"""
    cmd_prompt='StarCom> '
    blank_cmd_prompt=cmd_prompt+'\n'
    def __init__(self, cred, prefix = None):
        assert isinstance(cred, StarExecCredentials)
        self.prefix = prefix
        args = ['java','-jar', cred.starexeccommand()]
        self.pipe = subprocess.Popen(args, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=1)
        ln = self.readLine() # consume Last update = ..
        ln = self.readLine() # consume <blankline>
        retids = self.prompt('returnids')
        if retids != []:
            print "retids",  repr(retids)
        loginstr = 'login u='+cred.username()+' p='+cred.password()+' addr='+cred.address()
        resp = self.prompt(loginstr)
        if self.prefix is not None:
            print self.prefix,
        print "Logging", cred.username() , "into starexec ", cred.address(),
        if not self.loginResponseIsOkay(resp):
            print "login failed"
            print "resp", repr(resp)
            self.close()
            raise Exception("login failed", resp)
        else:
            print "login successful"


    def __enter__(self):
        assert self.is_open()
        return self;

    def __exit__(self, type, value, traceback):
        self.close()

    def is_open(self):
        return self.pipe.returncode == None

    def close(self):
        self.pipe.poll()
        if self.is_open():
            resp = self.pipe.communicate("logout")
            if self.prefix is not None:
                print self.prefix,
            print "Logging out of star exec", resp

    def readLine(self):
        """Do not call this from outside of the class"""
        assert self.is_open()
        return self.pipe.stdout.readline()

    def writeLine(self, line):
        """Do not call this from outside of the class"""
        assert self.is_open()
        print >> self.pipe.stdin, line
        self.pipe.stdin.flush()

    def prompt(self, cmd):
        self.writeLine(cmd) # send the command
        #consume exactly one command prompt
        first = self.readLine()
        ind = first.find(StarExecPipe.cmd_prompt)
        assert ind == 0
        pos = len(StarExecPipe.cmd_prompt)
        lines = []
        line = first[pos:]
        #print "sub", pos, repr(line)
        #print "first", repr(first)

        while line != '\n':
            #print "read", len(lines), repr(line)
            # stop at the first empty line
            lines.append(line)
            #print lines
            line = self.readLine()
        return lines

    def lssubspaces(self, space_id):
        lines = self.prompt('lssubspaces id='+str(space_id))
        #print lines
        return [ idnameMapIntoPairs(lineIntoMap(x)) for x in lines ]

    def lsbenchmarks(self, space_id):
        lines = self.prompt('lsbenchmarks id='+str(space_id))
        #print lines
        return [ idnameMapIntoPairs(lineIntoMap(x)) for x in lines ]

    def lssolvers(self, space_id):
        """lssolvers id=14436
        id=668 : name=test (recycled)
        id=669 : name=took2 (recycled)
        id=670 : name=took3 (recycled)
        id=671 : name=took4"""
        lines = self.prompt('lssolvers id='+str(space_id))
        pair_list = [ idnameMapIntoPairs(lineIntoMap(x)) for x in lines ]
        rec = [(x,y) for x,y in pair_list if y.endswith("(recycled)")]
        notrec = [(x,y) for x,y in pair_list if not y.endswith("(recycled)")]
        return rec, notrec

    def getjobpair(self, jobpair, out, ow):
        query = 'getjobpair id='+str(jobpair)+' out='+out
        if ow is not None:
            query += ' ow='
        #print "issuing", query
        resp = self.prompt(query)
        #print "getjobpair", resp

    def getjobinfo(self, job_id, out, ow):
        query = 'getjobinfo id='+str(job_id)+' out='+out
        query += ' incids='
        if ow is not None:
            query += ' ow='
        print query
        resp = self.prompt(query)
        print "getjobinfo", resp

    def loginResponseIsOkay(self, resp):
        return resp != 'ERROR: Invalid username and/or password\n'

