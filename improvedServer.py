import json
import uuid

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
        self.labelQIdMap = {label,qid_ for label,qid_ in self.labelQIdMap.items() if qid_ != qid }
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
    

#message class basically a object representation of json
#obtained from message pump
class Message:

    def __init__(self,uuid,msgType):
        self.id = uuid
        self.msgType = msgType
        self.api = None
        self.result = None
        self.sequenceNum = None
        self.params = None
        self.changesState = None
    
    def getJson(self):

        jsonrep = {'id':self.id, 'type':self.msgType}

        if self.api is not None:
            jsonrep['api'] = self.api

        if self.result is not None:
            jsonrep['result']= result
        
        if self.sequenceNum is not None:
            jsonrep['sequenceNum'] = sequenceNum
        
        if self.params is not None:
            jsonrep['params']= params

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

    def createMsg(msg):
        #create msg from data

        message = Message(msg['uuid'],msg['type'])
        
        if 'api' in msg.keys():
            message.api = msg['api']

        if 'result' in msg.keys():
            message.result = msg['result']

        if 'sequenceNum' in msg.keys():
            message.sequenceNum = msg['sequenceNum']

        if 'params' in msg.keys():
            message.params = msg['params']

        if 'changesState' in msg.keys():
            message.changesState = msg['changesState']

           

class FTQueueService:

    def __init__(self,nodenum,totalnodes,socket):
        self.nodenum = nodenum
        self.totalnodes = totalnodes
        self.socket = socket
        self.msgRespAddresses = {}
        self.lastSeenLSequences = [-1] * len(totalnodes)
        self.outstandingMessages = []
        self.LSequence = -1
        self.isLeader = (True if nodenum == 0 else False)
        self.highestSeenGSequence = -1
        self.lastSentSequenceMessage = None
        self.ftqueue = FTQueue()
        self.pendingRequestsReturnAddresses = {}

    def getNextMessage(self):
        # get next data from socket,create msg and retrun
        # call factory to get Message Object
        msg,sender = self.socket.recvfrom(4096)
        msgObj = MessageFactory.createMsg(json.loads(msg.decode('utf-8'))) 
        return msgObj, sender

    def sendMessage(self,message, address):
        #get message json
        jsonRep = message.getJson()

        #serialize and send to address
        serializedMsg = json.dumps(jsonRep).encode('utf-8')
        self.socket.sendto(serializedMsg,address)

    def broadcastMessage(self,message):
        #send message to all other nodes

        for n in range(self.totalnodes):
            if n != self.nodenum:
                self.sendMessage(message,('localhost',10000+n))

    def respondToClient(self,result,api,params,address):
        #respond to client@address with result of operation
        response = Message(str(uuid.uuid4()),"client response")
        response.result = result
        response.api = api
        response.params = params
        self.sendMessage(response,address)

    def run(self):
        message,sender = self.getNextMessage()
        self.isLeader = False

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
            
            if self.isLeader:
                #sequence any outstanding messages
                processOutstandingMessages()

            message, sender = self.getNextMessage()

    def retransmitMessage(message, sender,isproposal):
        #if proposal, broadcast to all
        if isproposal:
            #get the local id to retransmit
            lsequence = message.params[0]

            #search for the proposal with matching lsequence and retransmit
            for msg in self.outstandingMessages.values():
                if msg.sequenceNum == lsequence:
                    self.broadcastMessage(msg)
                    break
        else: 
            #not a proposal, retransmit last sequence message to sender
            self.sendMessage(lastSentSequenceMessage,sender)

    def processOutstandingMessages(self):
        #judge on outstanding proposal if present

    
    def doQueueOperation(self,api,params):
        #identify required op. do and return value
        if api=="qId":
            return self.ftqueue.qid(params[0])
        elif api=="qTop":
            return self.ftqueue.top(params[0])
        elif api=="qCreate":
            return self.ftqueue.create(params[0])
        elif api=="qDestroy":
            return self.ftqueue.destroy(params[0])
        elif api=="qPush":
            return self.ftqueue.push(params[0],params[1])
        elif api=="qPop":
            return self.ftqueue.pop(params[0])
        elif api=="qSize":
            return self.ftqueue.size(params[0])

    def handleStatechangingRequest(self,message,sender):
        # first check if you're the leader
        if self.isLeader:
            #create and send sequence message
            self.sendSequenceMessage(message)
            #do local operation
            result = self.doQueueOperation(message.api,message.params)
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
        sequencemsg.sequenceNum = self.highestSeenGSequence
        sequencemsg.api = message.api
        sequencemsg.params = message.params
        self.broadcastMessage(sequencemsg)

        #set last sent sequence message
        self.lastSentSequenceMessage = sequencemsg

        #reset leader status
        self.isLeader = False
    
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

    def sendRetransmitMessage(self,isproposal, params,address):
        #build retransmit message
        retransmit = Message(str(uuid.uuid4()),"retransmit proposal" if isproposal else "retransmit sequence")

        if isproposal:
            retransmit.params = params
        
        #send
        self.sendMessage(retransmit,address)


    def handleClientRequest(self,message,sender):
        #if non state changing request, process and return
        if not message.stateChanging:
            result = None
            try:
                result = self.doQueueOperation(message.api,message.params)
            except Exception as e:
                result = "Error occurred {0}".format(e)
            self.respondToClient(result,message.api,message.params,sender)
        else:
            self.handleStatechangingRequest(message,sender)

    def handleProposalMessage(self,message,sender):
        #update lsequence for the sender
        #send retransmit if out of order
        if self.lastSeenLSequences[message.sequenceNum[0]]+1 == message.sequenceNum[1]:
            self.lastSeenLSequences[message.sequenceNum[0]] += 1
        else:
            #send retransmit for all missing messages
            for num in range(self.lastSeenLSequences[message.sequenceNum[0]]+1,message.sequenceNum[1]+1):
                params = [num]
                self.sendRetransmitMessage(True,params,sender)
            return

        #if leader, go through executing state affecting operation approach
        if self.isLeader:
            #send sequence message
            self.sendSequenceMessage(message)
            #do operation locally
            self.doQueueOperation(message.api,message.params)

    def handleSequenceMessage(self,message,sender):
        #already processed
        if message.sequenceNum <= self.highestSeenGSequence:
            return

        #check if out of order
        if message.sequenceNum > self.highestSeenGSequence+1:
            #send retransmit request for missing messages
            nodestart = (self.highestSeenGSequence+1)%self.totalnodes
            nodeend = (message.sequenceNum)%self.totalnodes

            for node in range(nodestart, nodeend+1):
                self.sendRetransmitMessage(False,None,('localhost',10000+node))
            return

        #service the operation
        result = self.doQueueOperation(message.api,message.params)

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

