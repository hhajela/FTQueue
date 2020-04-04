import json
import socket
import sys
import pickle
import uuid
from threading import Timer

class FTQueue:

    def __init__(self):
        self.labelQIdMap = {}
        self.qidQMap = {}

    def create(self,label):
        #push a new entry if not already present
        if label not in self.labelQIdMap.keys():
            #create new queue
            newQid = len(self.qidQMap.keys())
            self.qidQMap[newQid] = []
            self.labelQIdMap[label] = newQid

        return self.labelQIdMap[label] # return qid

    def destroy(self,qid):
        #delete queue with qid and remove label association
        self.labelQIdMap = {label:qid_ for label,qid_ in self.labelQIdMap.items() if qid_ != qid }
        del self.qidQMap[qid]

    def qid(self,label):
        return self.labelQIdMap[label]

    def push(self,qid, num):
        #push a new entry into the corresponding queue
        self.qidQMap[qid].append(num)

    def pop(self,qid):
        #remove + return element from q
        return self.qidQMap[qid].pop(0)

    def top(self,qid):
        #return element from q
        return self.qidQMap[qid][0]
    
    def size(self,id):
        #return size of q
        return len(self.qidQMap[id])

    def toJson(self):
        return json.dumps(self.labelQIdMap), json.dumps(self.qidQMap)

    def fromJson(labeljsonrep, qidjsonrep):
        ftqueue = FTQueue()
        ftqueue.labelQIdMap = json.loads(labeljsonrep)
        
        qidQmap = json.loads(qidjsonrep)
        for key in qidQmap.keys():
            ftqueue.qidQMap[int(key)] = qidQmap[key].copy()

        return ftqueue

    

#message class basically a object representation of json
#obtained from message pump
class Message:

    def __init__(self,uuid,msgType):
        self.id = uuid
        self.msgType = msgType
        self.api = None
        self.result = None
        self.sequenceNum = None
        self.gSequenceNum = None
        self.params = None
        self.changesState = None
    
    def getJson(self):

        jsonrep = {'uuid':self.id, 'type':self.msgType}

        if self.api is not None:
            jsonrep['api'] = self.api

        if self.result is not None:
            jsonrep['result']= self.result
        
        if self.sequenceNum is not None:
            jsonrep['sequenceNum'] = self.sequenceNum

        if self.gSequenceNum is not None:
            jsonrep['gSequenceNum'] = self.gSequenceNum
        
        if self.params is not None:
            jsonrep['params']= self.params

        if self.msgType == "client request" and self.changesState is None:
            jsonrep['changesState'] = self.changesState

        return jsonrep

"""
Request dictionary

{
type : "retransmit proposal/retransmit sequence/proposal/sequence/client request/client response" # message types

result(optional field) : # result of operation

api (optional field) : "" # q apis

uuid : "<UUID>" # attached to every message


}
"""

class MessageFactory:
    @staticmethod
    def createMsg(msg):
        #create msg from data

        message = Message(msg['uuid'],msg['type'])
        
        if 'api' in msg.keys():
            message.api = msg['api']

        if 'result' in msg.keys():
            message.result = msg['result']

        if 'sequenceNum' in msg.keys():
            message.sequenceNum = msg['sequenceNum']

        if 'gSequenceNum' in msg.keys():
            message.gSequenceNum = msg['gSequenceNum']

        if 'params' in msg.keys():
            message.params = msg['params']

        if 'changesState' in msg.keys():
            message.changesState = msg['changesState']

        return message

class TimedThread:

    def __init__(self, duration, fn, args):
        self.timer = Timer(duration, self.tick)
        self.fn = fn
        self.args = args
        self.duration = duration
        self.cancelled = False
    
    def tick(self):
        #call callback
        if self.args is not None:
            self.fn(*self.args)
        else:
            self.fn()

        if not self.cancelled:
            self.timer = Timer(self.duration, self.tick)
            self.timer.start()

    def cancel(self):
        self.cancelled = True
        self.timer.cancel()

    def start(self):
        self.timer.start()

    def reset(self):
        self.timer.cancel()
        self.cancelled = False
        self.timer = Timer(self.duration, self.tick)
        self.timer.start()

class FTQueueService:

    def __init__(self,nodenum,totalnodes,socket):
        self.nodenum = nodenum
        self.totalnodes = totalnodes
        self.socket = socket
        self.lastSeenLSequences = [-1] * totalnodes
        self.outstandingMessages = {}
        self.LSequence = -1
        self.isLeader = (True if nodenum == 0 else False)
        self.highestSeenGSequence = -1
        self.lastSentSequenceMessage = None
        self.ftqueue = FTQueue()
        self.pendingRequestsReturnAddresses = {}
        self.curConfig = ''.join(str(i) for i in range(totalnodes))# what is the current membership
        self.knownMembers = [True] * totalnodes
        self.deliveredMessages = [[]] #keep track of all delivered msgs
        self.deliveredMessages[-1].append(self.curConfig)
        self.hbeatThread = TimedThread(20, self.sendHbeat, None)
        self.hbeatTimers = [TimedThread(30,self.hbeatTimeout,[i]) for i in range(totalnodes)]
        self.discoverMembers = False

    def getNextMessage(self):
        # get next data from socket,create msg and retrun
        # call factory to get Message Object
        msg,sender = self.socket.recvfrom(4096)
        msgJson = json.loads(msg.decode('utf-8'))
        log("Node {0} received msg {1}".format(self.nodenum,msgJson))
        msgObj = MessageFactory.createMsg(msgJson) 
        return msgObj, sender

    def sendMessage(self,message, address):
        #get message json
        jsonRep = message.getJson()
        log("Node {2} Sending message {0} to address {1}".format(jsonRep,address,self.nodenum))
        #serialize and send to address
        serializedMsg = json.dumps(jsonRep).encode('utf-8')
        self.socket.sendto(serializedMsg,address)

    def broadcastMessage(self,message):
        #send message to all other nodes
        log("Broadcasting message {0} to all".format(message.getJson()))
        for i in range(len(self.knownMembers)):
            if self.knownMembers[i] and i != self.nodenum:
                self.sendMessage(message,('localhost',10000+i))

    def respondToClient(self,result,api,params,address):
        #respond to client@address with result of operation
        response = Message(str(uuid.uuid4()),"client response")
        response.result = result
        response.api = api
        response.params = params
        self.sendMessage(response,address)

    def run(self):
        log("Server {0} running".format(self.nodenum))

        #start hbeat thread
        self.hbeatThread.reset()

        #start expiry timers for all members
        for i,timer in enumerate(self.hbeatTimers):
            if i != self.nodenum:
                timer.reset()

        message,sender = self.getNextMessage()

        while(message is not None):

            if message.msgType == "client request":
                self.handleClientRequest(message, sender)
            elif message.msgType == "proposal":
                self.handleProposalMessage(message,sender)
            elif message.msgType == "sequence":
                self.handleSequenceMessage(message,sender)
            elif message.msgType == "retransmit proposal":
                self.retransmitMessage(message,sender,True)
            elif message.msgType == "retransmit sequence":
                self.retransmitMessage(message,sender,False)
            elif message.msgType == "member discovery":
                self.doMemberDiscovery()
            elif message.msgType == "heartbeat":
                self.processHeartbeat(message, sender)
            
            if self.isLeader:
                #sequence any outstanding requests first
                self.processOutstandingMessages()

            log("Server {0} waiting for messages".format(self.nodenum))
            message, sender = self.getNextMessage()

    def retransmitMessage(self, message, sender,isproposal):
        #if proposal, broadcast to all
        if isproposal:
            #get the local id to retransmit
            lsequence = message.params[0]

            #search for the proposal with matching lsequence and retransmit
            for msg in self.outstandingMessages.values():
                if msg.sequenceNum[1] == lsequence:
                    self.broadcastMessage(msg)
                    break
        else: 
            #not a proposal, retransmit last sequence message to sender
            self.sendMessage(self.lastSentSequenceMessage,sender)

    def processOutstandingMessages(self):
        #judge on outstanding proposal if present
        if len(self.outstandingMessages.values()) == 0:
            return

        print("Node {0} processing outstanding messages".format(self.nodenum))
        
        message = list(self.outstandingMessages.values())[0]
        address = self.pendingRequestsReturnAddresses[message.id]

        #send sequence msg
        self.sendSequenceMessage(message)

        #do operation
        result = self.doQueueOperation(message.api,message.params)

        #update delivered messages and save state
        self.deliveredMessages[-1].append(message)
        self.saveAppState()

        #remove from outstanding message list
        del self.outstandingMessages[message.id]
        del self.pendingRequestsReturnAddresses[message.id]

        #respond to cliemt
        self.respondToClient(result,message.api,message.params,address)
    
    def doQueueOperation(self,api,params):
        #identify required op. do and return value,exception message if failure
        result = None
        try:
            if api=="qId":
                result = self.ftqueue.qid(params[0])
            elif api=="qTop":
                result = self.ftqueue.top(params[0])
            elif api=="qCreate":
                result = self.ftqueue.create(params[0])
            elif api=="qDestroy":
                result = self.ftqueue.destroy(params[0])
            elif api=="qPush":
                result = self.ftqueue.push(params[0],params[1])
            elif api=="qPop":
                result = self.ftqueue.pop(params[0])
            elif api=="qSize":
                result = self.ftqueue.size(params[0])

        except Exception as e:
                result = "Error occurred {0}".format(e)

        return result

    def handleStatechangingRequest(self,message,sender):
        # first check if you're the leader
        if self.isLeader:
            #create and send sequence message
            self.sendSequenceMessage(message)
            #do local operation
            result = self.doQueueOperation(message.api,message.params)

            #save state and update delivered messages
            self.deliveredMessages[-1].append(message)
            self.saveAppState()

            #return result to client
            self.respondToClient(result,message.api,message.params,sender)
        else:
            #send a proposal message
            self.sendProposalMessage(message,sender)
    
    def sendSequenceMessage(self,message):
        #broadcast sequence message to all
        sequencemsg = Message(message.id,"sequence")
        self.highestSeenGSequence += 1

        #set sequence message values
        sequencemsg.sequenceNum = message.sequenceNum
        sequencemsg.gSequenceNum = self.highestSeenGSequence
        sequencemsg.api = message.api
        sequencemsg.params = message.params
        self.broadcastMessage(sequencemsg)

        #set last sent sequence message
        self.lastSentSequenceMessage = sequencemsg

        #reset leader status
        self.isLeader = False
        log("{0} is no longer leader".format(self.nodenum))
    
    def sendProposalMessage(self,message,sender):
        #build proposal message and broadcast to all
        proposal = Message(message.id,"proposal")

        #set vals
        self.LSequence += 1
        proposal.sequenceNum = [self.nodenum,self.LSequence]
        proposal.api = message.api
        proposal.params = message.params

        #broadcast
        self.broadcastMessage(proposal)

        #add to outstanding messages
        self.outstandingMessages[proposal.id] = proposal
        self.pendingRequestsReturnAddresses[proposal.id]= sender

    def sendRetransmitMessage(self,isproposal, params, address):
        #build retransmit message
        retransmit = Message(str(uuid.uuid4()),"retransmit proposal" if isproposal else "retransmit sequence")

        if isproposal:
            retransmit.params = params
        
        #send
        self.sendMessage(retransmit,address)


    def handleClientRequest(self,message,sender):
        #if non state changing request, process and return
        log("client request received {0} from address {1}".format(message.getJson(),sender))
        if not message.changesState:
            result = self.doQueueOperation(message.api,message.params)
            self.respondToClient(result,message.api,message.params,sender)
        else:
            self.handleStatechangingRequest(message,sender)

    def handleProposalMessage(self,message,sender):
        #update lsequence for the sender
        #send retransmit if out of order

        #already seen message
        if self.lastSeenLSequences[message.sequenceNum[0]] >= message.sequenceNum[1]:
            log("Node {0} has already seen message {1} from node {2}".format(self.nodenum,message.getJson(),message.sequenceNum[0]))
            return

        if self.lastSeenLSequences[message.sequenceNum[0]]+1 == message.sequenceNum[1]:
            self.lastSeenLSequences[message.sequenceNum[0]] += 1
        else:
            #send retransmit for all missing messages
            log("sending retransmit proposal")
            for num in range(self.lastSeenLSequences[message.sequenceNum[0]]+1,message.sequenceNum[1]+1):
                params = [num]
                self.sendRetransmitMessage(True,params,sender)
            return

        log("{1} is leader : {0}".format(self.isLeader,self.nodenum))
        #if leader, go through executing state affecting operation approach
        if self.isLeader:
            #send sequence message
            self.sendSequenceMessage(message)

            #do operation locally
            self.doQueueOperation(message.api,message.params)

            #update delivered messages and save state
            self.deliveredMessages[-1].append(message)
            self.saveAppState()

    #update highest seen local and global sequence number
    def handleSequenceMessage(self,message,sender):
        #already processed
        if message.gSequenceNum <= self.highestSeenGSequence:
            return

        #check if out of order
        if message.gSequenceNum > self.highestSeenGSequence+1:
            #send retransmit request for missing messages
            for seqnum in range(self.highestSeenGSequence+1, message.gSequenceNum +1):
                port = (seqnum % self.totalnodes) + 10000
                self.sendRetransmitMessage(False,None,('localhost',port))
            return

        #service the operation
        result = self.doQueueOperation(message.api,message.params)

        #update local sequence number for original sender
        if message.sequenceNum is not None:
            self.lastSeenLSequences[message.sequenceNum[0]] = max(self.lastSeenLSequences[message.sequenceNum[0]],message.sequenceNum[1])

        #respond to client if present in outstanding messages
        if message.id in self.outstandingMessages.keys():
            #return result to client
            self.respondToClient(result,message.api,message.params,self.pendingRequestsReturnAddresses[message.id])

            #remove from outstanding messages
            del self.pendingRequestsReturnAddresses[message.id]
            del self.outstandingMessages[message.id]
        
        #change leader status if applicable
        self.highestSeenGSequence += 1
        if (self.highestSeenGSequence+1)%self.totalnodes == self.nodenum:
            self.isLeader = True
            log("{0} is now leader".format(self.nodenum))

        #add message to delivered message lst and save state
        self.deliveredMessages[-1].append(message)
        self.saveAppState()
    
    def saveAppState(self):
        #save data structure
        labeljson, qidjson = self.ftqueue.toJson()
        with open("label{0}.json".format(self.nodenum),'w') as f:
            f.write(labeljson)
        with open("qid{0}.json".format(self.nodenum),'w') as f:
            f.write(qidjson)

        #save known member list
        with open("members{0}.p".format(self.nodenum),"wb") as f:
            pickle.dump(self.knownMembers,f)

        #save current sequence and config number
        with open("sequence{0}.json".format(self.nodenum),"w") as f:
            f.write(json.dumps({'config':self.curConfig, 'sequence':self.highestSeenGSequence}))

        #save current message history
        with open("delivered{0}.p".format(self.nodenum), "wb") as f:
            pickle.dump(self.deliveredMessages, f)

    def doMemberDiscovery(self):
        #set member discover to true
        self.discoverMembers = True

        #stop hbeat timers and your own heartbeat thread
        for timer in self.hbeatTimers:
            timer.cancel()
        
        self.hbeatThread.cancel()
        
        #establish all members
        self.reachMembershipConsensus()
        
        #do same thing for sequence messages
        self.reachSequenceConsensus()

        #update current config
        self.curConfig = ''.join([str(i) for i in range(len(self.knownMembers)) if self.knownMembers[i]])
        
        #update sequence number
        self.deliveredMessages.append([])
        self.deliveredMessages[-1].append(self.curConfig)

        #reset 
        self.totalnodes = len(self.curConfig)
        self.lastSeenLSequences = [-1] * self.totalnodes
        self.outstandingMessages = {}
        self.LSequence = -1
        self.isLeader = (True if self.nodenum == 0 else False)
        self.highestSeenGSequence = -1
        self.lastSentSequenceMessage = None
        self.discoverMembers = False
        self.hbeatTimers = [TimedThread(30,self.hbeatTimeout,[i]) for i in range(len(self.knownMembers)) if self.knownMembers[i]]

    def reachMembershipConsensus(self):
        
        #send known member list
        message = Message(str(uuid.uuid4()), "member discovery")
        
        #initially you are only certain about your own membership
        #but broadcast to old known members
        newknownMembers = [i==self.nodenum for i in range(len(self.knownMembers))]
        message.params = newknownMembers
        message.sequenceNum = self.curConfig
        self.broadcastMessage(message)
        self.knownMembers = newknownMembers

        #change socket to non blcokign for message exchange
        #before checking all values same
        self.socket.setblocking(False)
        membershipSame = False
        othermembers = {}

        while not membershipSame:
            noUpdateCount = 0
            #exchange messages until no new updates
            while True:
                anyUpdate = False

                #wait 5 seconds
                time.sleep(5)

                #check for any new message
                try:
                    message, sender = self.getNextMessage()

                    #skip if not membership message or for another config or empty
                    if message.msgType != "member discovery" or message.sequenceNum != self.curConfig or message.params is None:
                        continue

                    #check message list is same as yours
                    #if not, update it
                    memberlist = message.params
                    othermembers[sender[1]-10000] = memberlist

                    for i in range(len(memberlist)):
                        if memberlist[i] and not self.knownMembers[i]:
                            self.knownMembers[i] = True
                            anyUpdate = True

                except Exception as e:
                    log("exception occurred while fetching message {0}".format(e))
                
                message = Message(str(uuid.uuid4()), "member discovery")
                message.params = self.knownMembers
                message.sequenceNum = self.curConfig
                self.broadcastMessage(message)

                if anyUpdate:
                    noUpdateCount = 0
                else:
                    noUpdateCount += 1

                log("no update count is {0}".format(noUpdateCount))
                #no new update since 5 times
                if noUpdateCount >= 5:
                    break

            membershipSame = True
            #check membership same or not
            log("checking membership {0} {1}".format(othermembers, self.knownMembers))
            for key in othermembers.keys():
                match = True
                for i in range(len(othermembers[key])):
                    if othermembers[key][i] != self.knownMembers[i]:
                        match = False
                        break

                if not match:
                    membershipSame = False
                    break
        
        self.socket.setblocking(True)
        print(self.knownMembers)

    def reachSequenceConsensus(self):
        #store old values
        oldMessageInfo = (len(self.deliveredMessages), len(self.deliveredMessages[-1]))

        #send message history
        message = Message(str(uuid.uuid4()), "sequence sync")
        message.params = [[msg.getJson() if i!=0 else msg for i,msg in enumerate(configMsgs)] for configMsgs in self.deliveredMessages]
        self.broadcastMessage(message)

        self.socket.setblocking(False)
        sequenceSynced = False
        otherVals = {}

        while not sequenceSynced:
            noUpdateCount = 0
            #exchange messages until timeout expires
            while True:
                anyUpdate = False

                #wait for fixed duration
                time.sleep(5)

                #check for any new msg
                try:
                    message, sender = self.getNextMessage()

                    #skip if not sequence sync message
                    if message.msgType != 'sequence sync':
                        continue
                
                    #unserialize to get list of list of msgs
                    serializedHistory = message.params
                    othermsgs = [[MessageFactory.createMsg(msg) if i!=0 else msg for i,msg in enumerate(configMsgs)] for configMsgs in serializedHistory]

                    #store total configs, total msgs in last config
                    otherVals[sender[1]-10000] = (len(othermsgs), len(othermsgs[-1]))

                    #check if this has any messages you don't
                    if len(othermsgs) >= len(self.deliveredMessages):
                        for i in range(len(othermsgs)):
                            if i < len(self.deliveredMessages) and len(othermsgs[i]) == len(self.deliveredMessages[i]):
                                continue

                            anyUpdate = True
                            if i>= len(self.deliveredMessages):
                                self.deliveredMessages.append([])
                                self.deliveredMessages[-1].append(othermsgs[i][0])
                            
                            if len(self.deliveredMessages[i])>1:
                                # copy what's missing
                                for j in range(len(self.deliveredMessages[i]),len(othermsgs[i])):
                                    self.deliveredMessages[-1].append(othermsgs[i][j])
                            else:
                                #now copy all missing messages
                                for j in range(1,len(othermsgs[i])):
                                    self.deliveredMessages[-1].append(othermsgs[i][j])
                except Exception as e:
                    log("error while fetching message {0}".format(e))

                #send new list again to randomly picked node
                message = Message(str(uuid.uuid4()), "sequence sync")
                message.params = [[msg.getJson() if i!=0 else msg for i,msg in enumerate(configMsgs)] for configMsgs in self.deliveredMessages]
                self.broadcastMessage(message)

                if anyUpdate:
                    noUpdateCount = 0
                else:
                    noUpdateCount += 1

                log("no update count is {0}".format(noUpdateCount))
                if noUpdateCount>=5:
                    break

            sequenceSynced = True
            #check messages same or not
            for node, messageInfo in otherVals.items():
                if messageInfo[0] != len(self.deliveredMessages) or messageInfo[1] != len(self.deliveredMessages[-1]):
                    log("sequence sync failed, {0} is not equal to {1}".format(messageInfo, (len(self.deliveredMessages), len(self.deliveredMessages[-1]))))
                    sequenceSynced = False
        
        self.socket.setblocking(True)
        self.playbackMessages(oldMessageInfo)

    
    def playbackMessages(self, oldMessageInfo):
        #playback all till end

        startIndex = oldMessageInfo[1]-1
        endIndex = len(self.deliveredMessages[oldMessageInfo[0]-1])

        #first deliver all remaining messages from last known config
        for i in range(startIndex,endIndex):
            msg = self.deliveredMessages[oldMessageInfo[0]-1][i]
            self.doQueueOperation(msg.api,msg.params)

        #process remaining
        for i in range(oldMessageInfo[0], len(self.deliveredMessages)):
            for j in range(1,len(self.deliveredMessages[i])):
                msg = self.deliveredMessages[i][j]
                self.doQueueOperation(msg.api,msg.params)
        

    def restart(self):
        #load data state
        labeljson = None
        qidjson = None

        with open("label{0}.json".format(self.nodenum),"r") as f:
            labeljson = f.read()

        with open("qid{0}.json".format(self.nodenum),"r") as f:
            qidjson = f.read()

        self.ftqueue = FTQueue.fromJson(labeljson, qidjson)

        #restore member list
        with open("members{0}.p".format(self.nodenum),"rb") as f:
            self.knownMembers = pickle.load(f)
        
        #restore sequence information
        with open("sequence{0}.json".format(self.nodenum), "r") as f:
            sequenceInfo = json.loads(f.read())
            self.curConfig = sequenceInfo['config']
            self.highestSeenGSequence = sequenceInfo['sequence']

        #restore messages delivered
        with open("delivered{0}.p".format(self.nodenum),"rb") as f:
            self.deliveredMessages = pickle.load(f)

        #start member discovery using restored member list as basepoint
        self.doMemberDiscovery()

    def sendHbeat(self):
        #send hbeat message to all
        message = Message(str(uuid.uuid4()), "heartbeat")
        self.broadcastMessage(message)

    def hbeatTimeout(self, nodenum):
        #hbeat for node x failed
        #initiate member discovery phase after marking this one as unknown
        self.knownMembers[nodenum] = False

        #start member discovery
        self.doMemberDiscovery()

    def processHeartbeat(self,message,sender):
        #reset timer for that node
        nodenum = sender[1]
        self.hbeatTimers[nodenum-10000].reset()


gLogfile = None

def log(text):
    global gLogfile
    with open(gLogfile,'a') as f:
        f.write(text+"\n")


if __name__=="__main__":
    #args
    nodenum = 0 if len(sys.argv)<2 else int(sys.argv[1])

    restart = False if len(sys.argv)<3 else sys.argv[2]=="-r"

    gLogfile = "log{0}.txt".format(nodenum)

    #create socket and bind
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the socket to the port
    server_address = ('localhost', nodenum+10000)
    sock.bind(server_address)

    #create service instance
    ftqueueService = FTQueueService(nodenum,5,sock)
    
    if restart:
        ftqueueService.restart()

    ftqueueService.run()