class FTQueue:

    def __init__(self):
        self.labelQIdMap = {};
        self.qidQMap = {};

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



class MessageFactory:

    def createMsg(msg):
        #create msg from data

class FTQueueService:

    def __init__(self,nodenum,totalnodes,socket):
        self.nodenum = nodenum
        self.totalnodes = totalnodes
        self.socket = socket
        self.msgRespAddresses = {}


    def getNextMessage(self):
        # call factory to get Message Object
        msg,sender = self.socket.recvfrom(4096)
        msgObj = MessageFactory.createMsg(msg)
        msgRespAddresses[msgObj.id] = sender 
        return msgObj



    def sendMessage(message, address):
        #get message json
        jsonRep = message.getJson()

        #serialize and send to address
        serializedMsg = json.dumps(jsonRep).encode('utf-8')
        self.socket.sendto(serializedMsg,address)

    
    




    