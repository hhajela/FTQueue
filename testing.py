import socket
import json
import uuid

if __name__=="__main__":
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

    print("creating q")
    #create q
    request = {'uuid':str(uuid.uuid4()),'type':'client request', 'api': 'qCreate', 'params' :[3], 'changesState':True}
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    input("Press Enter to continue...")
    
    print("")
    print("get qid")
    request['api'] = 'qId'
    request['changesState'] = False
    request['params'] = [3]
    request['uuid'] = str(uuid.uuid4())
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    response = json.loads(sock.recvfrom(4096)[0].decode('utf-8'))
    qid = response['result']
    print(response)
    """
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    print(sock.recvfrom(4096))
    """

    input("Press Enter to continue...")

    print("")
    print("doing push")
    #push value
    request['uuid'] = str(uuid.uuid4())
    request['api'] = 'qPush'
    request['changesState'] = True
    request['params'] = [qid,20]
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("checking top")
    #check top for two nodes
    request['uuid'] = str(uuid.uuid4())
    request['api'] = 'qTop'
    request['changesState'] = False
    request['params'] = [qid]
    print("request {0}".format(request))
    print("response:-")
    #sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    #print(sock.recvfrom(4096))
    
    
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    
    """
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    print(sock.recvfrom(4096))
    """

    input("Press Enter to continue...")

    print("")
    print("doing pop")
    #do pop
    request['uuid'] = str(uuid.uuid4())
    request['api'] = 'qPop'
    request['changesState'] = True
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    """
    print("")
    print("checking queue size")
    #check size for two nodes
    request['uuid'] = str(uuid.uuid4())
    request['api'] = 'qSize'
    request['changesState'] = False
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    print(sock.recvfrom(4096))
    """
    input("Press Enter to continue...")

    print("")
    print("creating another q")
    #create another q
    request = {'type':'client request', 'api': 'qCreate', 'params' :[9]}
    request['changesState'] = True
    request['uuid'] = str(uuid.uuid4())
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    """
    print("")
    print("checking its existence")
    request['api'] = 'qId'
    request['changesState'] = False
    request['uuid'] = str(uuid.uuid4())
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    print(sock.recvfrom(4096))
    """
    input("Press Enter to continue...")
    print("")
    print("deleting q")
    #create another q
    request = {'type':'client request', 'api': 'qDestroy', 'params' :[1]}
    request['uuid'] = str(uuid.uuid4())
    request['changesState'] = True
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    
    input("Press Enter to continue...")
    print("")
    print("verifying deletion")
    request['api'] = 'qId'
    request['paras'] = [9]
    request['changesState'] = False
    request['uuid'] = str(uuid.uuid4())
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    #sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    #print(sock.recvfrom(4096))



