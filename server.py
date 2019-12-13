import argparse
import sched
import xmlrpc.client
from hashlib import sha256
from random import randrange
from socketserver import ThreadingMixIn
from threading import Thread
from time import time, sleep
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xmlrpc.server import SimpleXMLRPCServer


class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


class threadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass


FileInfoMap = {}
blocks = {}
serverNum = -1
servers = []
currentTerm = 0
votedFor = -1
log = []
commitIndex = -1
lastApplied = -1
matchIndex = []
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2
CRASHED = 3
timer = 0
state = FOLLOWER
timeout = randrange(400, 700) / 1000


# A simple ping, returns true
def ping():
    """A simple ping method"""
    print("Ping()")
    return True


# Gets a block, given a specific hash value
def getblock(h):
    """Gets a block"""
    print("GetBlock(" + h + ")")
    if h in blocks:
        blockData = blocks[h]
        return blockData
    else:
        print("block not found")


# Puts a block
def putblock(b):
    """Puts a block"""
    print("PutBlock()")
    blocks[sha256(b.data).hexdigest()] = b
    return True


# Given a list of blocks, return the subset that are on this server
def hasblocks(blocklist):
    """Determines which blocks are on this server"""
    print("HasBlocks()")
    return list(filter(lambda x: x in blocks, blocklist))


# Retrieves the server's FileInfoMap
def getfileinfomap():
    """Gets the fileinfo map"""
    print("GetFileInfoMap()")
    return FileInfoMap


def toFollower(term):
    global currentTerm, state, timer, votedFor
    state = FOLLOWER
    currentTerm = term
    votedFor = -1
    timer = time()


def replicate(serverId, successes):
    while successes[0] + 1 <= (len(servers) + 1) / 2:
        if state == LEADER:
            try:
                term, success = servers[serverId].surfstore.appendEntries(currentTerm, log, commitIndex)
                # print(serverId, log)
                if success:
                    successes[0] += 1
                    matchIndex[serverId] = len(log) - 1
                    break
                elif term != -1:
                    raise Exception('replicate failed')
                if term > currentTerm:
                    toFollower(term)
            except Exception as e:
                print(str(e))


# Update a file's fileinfo entry
def updatefile(filename, version, blocklist):
    global commitIndex
    """Updates a file's fileinfo entry"""
    if state != LEADER or state == CRASHED:
        raise Exception("not leader or crashed")
    print("UpdateFile()")
    if filename in FileInfoMap:
        v = FileInfoMap[filename][0]
        if blocklist == "0" and FileInfoMap[filename][1] == "0":
            return v
        elif version != v + 1:
            print("version too old")
            return False

    successes = [0]
    entry = (currentTerm, (filename, version, blocklist))
    log.append(entry)
    for i in range(len(servers)):
        th = Thread(target=replicate, args=(i, successes))
        th.start()
    while successes[0] + 1 <= (len(servers) + 1) / 2:
        if state == CRASHED:
            return 0
    commitIndex += 1
    FileInfoMap[filename] = (version, blocklist)
    sleep(0.5)
    return version


# PROJECT 3 APIs below

# Queries whether this metadata store is a leader
# Note that this call should work even when the server is "crashed"
def isLeader():
    """Is this metadata store a leader?"""
    print("IsLeader()")
    return state == LEADER


# "Crashes" this metadata store
# Until Restore() is called, the server should reply to all RPCs
# with an error (unless indicated otherwise), and shouldn't send
# RPCs to other servers
def crash():
    """Crashes this metadata store"""
    global state
    print("Crash()")
    state = CRASHED
    return True


# "Restores" this metadata store, allowing it to start responding
# to and sending RPCs to other nodes
def restore():
    """Restores this metadata store"""
    global state, timer
    print("Restore()")
    state = FOLLOWER
    timer = time()
    return True


# "IsCrashed" returns the status of this metadata node (crashed or not)
# This method should always work, even when the node is crashed
def isCrashed():
    """Returns whether this node is crashed or not"""
    print("IsCrashed()")
    return state == CRASHED


# Requests vote from this server to become the leader
def requestVote(term, candidateId, lastLogIndex, lastLogTerm):
    """Requests vote to be the leader"""
    global timer, currentTerm, state, timer, votedFor

    if state == CRASHED:
        return -1, False

    res = False
    if currentTerm < term:
        state = FOLLOWER
        currentTerm = term
        votedFor = -1
    if currentTerm == term:
        if (votedFor == -1 or votedFor == candidateId) and \
                (not log or lastLogIndex >= len(log) - 1 and lastLogTerm >= log[-1][0]):
            votedFor = candidateId
            timer = time()
            res = True
    return currentTerm, res


def commit():
    global lastApplied
    # print('commit', lastApplied, commitIndex)
    while lastApplied < commitIndex:
        lastApplied += 1
        _, (filename, version, blocklist) = log[lastApplied]
        FileInfoMap[filename] = (version, blocklist)


# Updates fileinfomap
def appendEntries(term, entries, leaderCommit):
    """Updates fileinfomap to match that of the leader"""
    if state == CRASHED:
        return -1, False
    global log, commitIndex, lastApplied, timer
    res = False
    print('appendEntries', leaderCommit, commitIndex, entries)
    if term >= currentTerm:
        if term > currentTerm:
            toFollower(term)
        else:
            timer = time()
        res = True
        log = entries
    if leaderCommit > commitIndex:
        commitIndex = min(leaderCommit, len(log) - 1)

    return currentTerm, res


def tester_getversion(filename):
    return FileInfoMap[filename][0]


def getVote(serverId, votes):
    global state, matchIndex
    try:
        term, voteGranted = servers[serverId].surfstore.requestVote(currentTerm, serverNum, len(log) - 1,
                                                                    log[-1][0] if log else 0)
        if state == CANDIDATE:
            if voteGranted:
                # prevent votes of previous election polluting current election
                if term == currentTerm:
                    votes[0] += 1
                    if votes[0] + 1 > (len(servers) + 1) / 2:
                        state = LEADER
                        matchIndex = [-1] * len(servers)
            else:
                if term > currentTerm:
                    toFollower(term)
    except Exception as e:
        print(str(e))


def heartbeat(sc):
    global commitIndex
    if state == LEADER:
        print('heartbeat', log, commitIndex)
        for i, server in enumerate(servers):
            try:
                term, success = server.surfstore.appendEntries(currentTerm, log, commitIndex)
                if success:
                    matchIndex[i] = len(log) - 1
                elif term != -1:
                    raise Exception('heartbeat failed')
                if term > currentTerm:
                    toFollower(term)
                    return
            except Exception as e:
                print(str(e))
        N = commitIndex + 1
        while (N < len(log) and log[N][0] == currentTerm and sum(i >= N for i in matchIndex) + 1 >
               (len(servers) + 1) / 2):
            N += 1
        if commitIndex < N - 1:
            commitIndex = N - 1
            commit()
        sc.enter(0.15, 1, heartbeat, (sc,))


# Reads the config file and return host, port and store list of other servers
def readconfig(config, servernum):
    """Reads cofig file"""
    fd = open(config, 'r')
    l = fd.readline()

    maxnum = int(l.strip().split(' ')[1])

    if servernum >= maxnum or servernum < 0:
        raise Exception('Server number out of range.')

    d = fd.read()
    d = d.splitlines()
    host = 0
    port = 0
    for i in range(len(d)):
        hostport = d[i].strip().split(' ')[1]
        if i == servernum:
            host = hostport.split(':')[0]
            port = int(hostport.split(':')[1])

        else:
            serverlist.append(hostport)

    return maxnum, host, port


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="SurfStore server")
        parser.add_argument('config', help='path to config file')
        parser.add_argument('servernum', type=int, help='server number')

        args = parser.parse_args()

        config = args.config
        servernum = args.servernum

        # server list has list of other servers
        serverlist = []

        # config = 'config.txt'
        # servernum = 2
        # maxnum is maximum number of servers
        maxnum, host, port = readconfig(config, servernum)

        print("Attempting to start XML-RPC Server...")
        print(host, port)
        server = threadedXMLRPCServer((host, port), requestHandler=RequestHandler)
        server.register_introspection_functions()
        server.register_function(ping, "surfstore.ping")
        server.register_function(getblock, "surfstore.getblock")
        server.register_function(putblock, "surfstore.putblock")
        server.register_function(hasblocks, "surfstore.hasblocks")
        server.register_function(getfileinfomap, "surfstore.getfileinfomap")
        server.register_function(updatefile, "surfstore.updatefile")
        # Project 3 APIs
        server.register_function(isLeader, "surfstore.isLeader")
        server.register_function(crash, "surfstore.crash")
        server.register_function(restore, "surfstore.restore")
        server.register_function(isCrashed, "surfstore.isCrashed")
        server.register_function(requestVote, "surfstore.requestVote")
        server.register_function(appendEntries, "surfstore.appendEntries")
        server.register_function(tester_getversion, "surfstore.tester_getversion")

        for hostport in serverlist:
            connection = xmlrpc.client.ServerProxy("http://" + hostport + '/RPC2', use_builtin_types=True)
            servers.append(connection)
        serverNum = servernum
        timer = time()


        def daemon():
            global state, currentTerm, timer, votedFor
            while True:
                # print(state, currentTerm, timer)
                # sleep(1)
                if state != CRASHED:
                    commit()

                if state == FOLLOWER:
                    if time() - timer > timeout:
                        state = CANDIDATE

                elif state == CANDIDATE:
                    if time() - timer > timeout:
                        print('candidate timeout', timeout)
                        currentTerm += 1
                        timer = time()
                        votedFor = serverNum
                        votes = [0]
                        for i in range(len(servers)):
                            th = Thread(target=getVote, args=(i, votes))
                            th.start()
                            if state != CANDIDATE:
                                break

                elif state == LEADER:
                    s = sched.scheduler(time, sleep)
                    s.enter(0, 1, heartbeat, (s,))
                    s.run()


        p = Thread(target=daemon)
        p.start()

        print("Started successfully.")
        print("Accepting requests. (Halt program to stop.)")
        server.serve_forever()




    except Exception as e:
        print("Server: " + str(e))
