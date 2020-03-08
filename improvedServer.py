import json
import uuid

class FTQueue:

    def __init__(self):
        self.labelQIdMap = {}
        self.qidQMap = {}

    def create(label):
        #push a new entry if not already present
        if label not in self.labelQIdMap.keys():
            #create new queue
            newQid = len(self.qidQMap.keys())
            self.qidQMap[newQid] = []
            self.labelQIdMap[label] = newQid

        return self.labelQIdMap[label] # return qid

    def destroy(qid):
        #delete queue with qid and remove label association
        self.labelQIdMap = {label,qid_ for label,qid_ in self.labelQIdMap.items() if qid_ != qid }
        del self.qidQMap[qid]

    def qid(label):
        return self.labelQIdMap[]

    def push(qid, num):
        #push a new entry into the corresponding queue
        self.qidQMap[qid].append(num)

    def pop(qid):
        #remove + return element from q
        return self.qidQMap[qid].pop(0)

    def top(qid):
        #return element from q
        return self.qidQMap[qid][0]
    

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
    
    def getJson():

        jsonrep = {'id':self.id, 'type':self.msgType}

        if self.api is not None:
            jsonrep['api'] = self.api

        if self.result is not None:
            jsonrep['result']= result
        
        if self.sequenceNum is not None:
            jsonrep['sequenceNum'] = sequenceNum
        
        if self.params is not None:
            jsonrep['params']= params

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

           

class FTQueueService:

    def __init__(self,nodenum,totalnodes,socket):
        self.nodenum = nodenum
        self.totalnodes = totalnodes
        self.socket = socket
        self.msgRespAddresses = {}
        self.lastSeenLSequences = [[] for i in range(len(totalnodes))]
        self.outstandingMessages = []
        self.LSequence = -1
        self.isLeader = (True if nodenum == 0 else False)
        self.highestSeenGSequence = -1
        self.lastSentSequenceMessage

    def getNextMessage(self):
        # get next data from socket,create msg and retrun
        # call factory to get Message Object
        msg,sender = self.socket.recvfrom(4096)
        msgObj = MessageFactory.createMsg(msg) 
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

    def respondToClient(self,result,address):
        #respond to client@address with result of operation
        response = Message(str(uuid.uuid4()),"client response")
        response.result = result
        self.sendMessage(response,address)

    def run(self):
        message,sender = self.getNextMessage()
        self.isLeader = False

        while(message is not None):
            if message.msgType == "client request":
                handleClientRequest(message, sender)
            elif message.msgType == "proposal":
                handleProposalMessage(message,sender)
            elif message.msgType == "sequence":
                handleSequenceMessage(message,sender)
            elif message.msgType == "retransmit proposal":
                retransmitMessage(message,sender,True)
            elif message.msgType == "retransmit sequence":
                retransmitMessage(message,sender,False)
            
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
            for msg in self.outstandingMessages:
                if msg.sequenceNum == lsequence:
                    self.broadcastMessage(msg)
        else: #not a proposal, retransmit last sequence message to sender
            self.sendMessage(lastSentSequenceMessage,sender)


    def processOutstandingMessages(self):
        #judge on outstanding proposal if present




    