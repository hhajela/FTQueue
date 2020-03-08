import socket
import sys
import json
import uuid

#initialize shared data structure
queueIDs = {}
sharedQueues = {}

# Create a TCP/IP socket
sock = None

#global sequence number seen till now
highestSeqNum = -1

#last sent sequence message for handling missed message cases
lastSentSequence = None

#is this node the leader
isLeader = False

totalNodes = 5

nodeNum = 0

#client requests not serviced yet
outstandingMessages = []
             
def respondToClient(result,senderAddress):
    global sock
    response = { 'response' : result}

    sock.sendto(json.dumps(response).encode('utf-8'),senderAddress)

def handleClientMessage(message, senderAddress):
    global sock
    global isLeader
    global outstandingMessages
    #if message requests queue id, top element or size, then just return
    messageapi = message['api']
    if messageapi == "qId":
        getQueueId(message,senderAddress)
    elif messageapi == "qSize":
        getQueueSize(message, senderAddress)
    elif messageapi == "qTop":
        getQueueTop(message, senderAddress)
    elif messageapi == "qPush":
        if isLeader:
            sequenceAndBroadcastMessage(message)
            result = None
            doQueuePush(message)
            respondToClient(result,senderAddress)
        else:
            sendProposalMessage(message)
            outstandingMessages.append((message,senderAddress))
    elif messageapi == "qPop":
        if isLeader:
            sequenceAndBroadcastMessage(message)
            result = None
            result = doQueuePop(message)
            respondToClient(result,senderAddress)
        else:
            sendProposalMessage(message)
            outstandingMessages.append((message,senderAddress))
    elif messageapi == "qCreate":
        if isLeader:
            sequenceAndBroadcastMessage(message)
            result = None
            result = doQueueCreate(message)
            respondToClient(result, senderAddress)
        else:
            sendProposalMessage(message)
            outstandingMessages.append((message,senderAddress))
    elif messageapi == "qDestroy":
        if isLeader:
            sequenceAndBroadcastMessage(message)
            result = None
            doQueueDestroy(message)
            respondToClient(result,senderAddress)
        else:
            sendProposalMessage(message)
            outstandingMessages.append((message,senderAddress))
    else:
        #return bad request error
        print("boo")

def getQueueId(message, senderAddress):
    global queueIDs
    global sock
    response = None
    if message['params'][0] in queueIDs.keys():
        #send correct queueId back to client
        response = { 'response' : queueIDs[message['params'][0]] }
    else:
        #send -1 to client
        response = { 'response' : -1 }
    
    print("sent response {0} to address {1}".format(response, senderAddress))
    sock.sendto(json.dumps(response).encode('utf-8'),senderAddress)


def getQueueSize(message, senderAddress):
    #send size of associated queue
    global sharedQueues
    global sock
    response = None
    qId = message['params'][0]
    if qId in sharedQueues.keys():
        #send correct queue size back to client
        response = { 'response' :  len(sharedQueues[qId])}
    else:
        #send -1 to client if queueId invalid
        response = { 'response' : -1 }
    
    print("getQueueSize sent response {0} to address {1}".format(response, senderAddress))
    sock.sendto(json.dumps(response).encode('utf-8'),senderAddress)


def getQueueTop(message, senderAddress):
    #send size of associated queue
    global sharedQueues
    global sock
    response = None
    qId = message['params'][0]
    if qId in sharedQueues.keys():
        #send queue top element back to client
        response = { 'response' :  sharedQueues[qId][0] }
    else:
        #send -1 to client if queueId invalid
        response = { 'response' : -1 }
    
    print("getQueueTop sent response {0} to address {1}".format(response, senderAddress))
    sock.sendto(json.dumps(response).encode('utf-8'),senderAddress)


def handleSequenceMessage(message, senderAddress):
    #seq message
    #if missing a number, send NACK to respective node
    #if correct increment higestSeqNum, service operation
    #
    global outstandingMessages
    global highestSeqNum
    global totalNodes
    global sock
    global nodeNum
    global isLeader

    sequenceNumber = message['sequenceNumber']

    #already processed
    if sequenceNumber <= highestSeqNum:
        return

    if sequenceNumber > highestSeqNum+1:
        #Not what we expected, send nack to get missing messages
        message = { 'type' : 'retransmit'}
        
        firstNode = (highestSeqNum+1) % totalNodes
        lastNode = sequenceNumber % totalNodes
        for nodePort in range(10000 + firstNode,10000 + lastNode+1):
            print("sending retransmit to address ('localhost',{0})".format(nodePort))
            sock.sendto(json.dumps(message).encode('utf-8'),('localhost',nodePort))
        return

    #process operation
    api = message['api']

    result = None
    if api == "qPush":
        doQueuePush(message)
    elif api == "qPop":
        result = doQueuePop(message)
    elif api == "qCreate":
        result = doQueueCreate(message)
    elif api == "qDestroy":
        doQueueDestroy(message)

    response = None
    #check if sequence message is present in outstanding messages, if so send response
    #then remove
    for msg,address in outstandingMessages:
        if msg['id'] == message['id']:
            #send response to client
            response=  {'response' : result}
            sock.sendto(json.dumps(response).encode('utf-8'),address)
            break
    
    outstandingMessages = [item for item in outstandingMessages if item[0]['id'] != message['id']]

    #increment highest seen seq num
    highestSeqNum += 1
    if (highestSeqNum+1)%totalNodes == nodeNum:
        isLeader = True
        print("{0} is now leader".format(nodeNum))
            


def handleRetransmitMessage(senderAddress):
    #nack from node, send latest sequence number message again
    global sock
    global lastSentSequence
    sock.sendto(json.dumps(lastSentSequence).encode('utf-8'),senderAddress)
    print("retransmitting last sent sequence message {0} to address {1}".format(lastSentSequence,senderAddress))


def doQueuePush(message):
    global sharedQueues
    qId = message['params'][0]
    value = message['params'][1]

    print("pushing value {0} into queue ID {1}".format(value,qId))
    sharedQueues[qId].append(value)


def doQueueCreate(message):
    global queueIDs
    global sharedQueues

    label = message['params'][0]

    if label in queueIDs.keys():
        return queueIDs[label]
    else:
        newId = len(sharedQueues.keys())+1
        queueIDs[label] = newId
        sharedQueues[newId] = []
        return newId


def doQueueDestroy(message):
    global sharedQueues

    qId = message['params'][0]

    try:
        del sharedQueues[qId]
    except KeyError as err:
        pass


def doQueuePop(message):
    global sharedQueues

    qId = message['params'][0]
    return sharedQueues[qId].pop(0)


def sendProposalMessage(message):
    global sock
    global totalNodes
    global nodeNum

    #broadcast to all nodes
    message['type'] = 'proposal'
    message['id'] = str(uuid.uuid4())

    print("broadcasting proposal message {0} to all".format(message))

    for node in range(totalNodes):
        if node != nodeNum:
            sock.sendto(json.dumps(message).encode('utf-8'),('localhost',10000+node))



def handleProposalMessage(message,address):
    global isLeader
    global outstandingMessages
    #if the leader, then respond by sequencing and broadcasting message
    if isLeader:
        sequenceAndBroadcastMessage(message)

        #process operation
        api = message['api']

        result = None
        if api == "qPush":
            doQueuePush(message)
        elif api == "qPop":
            result = doQueuePop(message)
        elif api == "qCreate":
            result = doQueueCreate(message)
        elif api == "qDestroy":
            doQueueDestroy(message)

        response = None
        #check if sequence message is present in outstanding messages, if so send response
        #then remove
        for msg,address in outstandingMessages:
            if msg['id'] == message['id']:
                #send response to client
                response=  {'response' : result}
                sock.sendto(json.dumps(response).encode('utf-8'),address)
                break
        
        outstandingMessages = [item for item in outstandingMessages if item[0]['id'] != message['id']]
    
    #otherwise dont do anything

def sequenceAndBroadcastMessage(message):
    global highestSeqNum
    global lastSentSequence
    global sock
    global totalNodes
    global nodeNum
    global isLeader

    if 'id' not in message.keys():
        message['id'] = str(uuid.uuid4())

    #increment sequence number and broadcast to all
    highestSeqNum +=1
    message['sequenceNumber'] = highestSeqNum
    message['type'] = 'sequence'

    print("broadcasting sequence number {0} for message {1} to all".format(highestSeqNum,message))

    for node in range(totalNodes):
        if node != nodeNum:
            sock.sendto(json.dumps(message).encode('utf-8'),('localhost',10000+node))
    
    #set last sent sequence to this message
    lastSentSequence = message

    #reset leader status
    isLeader = False
    print("{0} is no longer leader".format(nodeNum))


if __name__=="__main__":
    
    nodeNum = int(sys.argv[1]) if len(sys.argv) >1 else 0

    #initial leader is 0th node
    if nodeNum == 0:
        isLeader = True
        print("{0} is leader".format(nodeNum))

    #decide port number based on node number
    port = nodeNum+10000
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the socket to the port
    server_address = ('localhost', port)
    print('starting up on %s port %s' % server_address)
    sock.bind(server_address)

    while True:
        print('\n Server {0} waiting to receive message'.format(nodeNum))
        data, senderAddress = sock.recvfrom(4096)
        
        print('received %s bytes from %s' % (len(data), senderAddress))
        print(data)
        
        if data:
            message = json.loads(data.decode('utf-8'))
            messagetype = message['type']

            if messagetype == "client":
                handleClientMessage(message,senderAddress)
            elif messagetype == "proposal":
                handleProposalMessage(message,senderAddress)
            elif messagetype == "sequence":
                handleSequenceMessage(message,senderAddress)
            elif messagetype == "retransmit":
                handleRetransmitMessage(senderAddress)
        else:
            print("Empty message?")


class FTQueue
{
    

}