import socket
import json

if __name__=="__main__":
    sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)

    print("creating q")
    #create q
    request = {'type':'client', 'api': 'qCreate', 'params' :[3]}
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("doing push")
    #push value
    request['api'] = 'qPush'
    request['params'] = [1,20]
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("checking top")
    #check top for two nodes
    request['api'] = 'qTop'
    request['params'] = [1]
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10000))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10002))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10003))
    print(sock.recvfrom(4096))
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10004))
    print(sock.recvfrom(4096))


    print("")
    print("doing pop")
    #do pop
    request['api'] = 'qPop'
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("checking queue size")
    #check size for two nodes
    request['api'] = 'qSize'
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

    print("")
    print("creating another q")
    #create another q
    request = {'type':'client', 'api': 'qCreate', 'params' :[9]}
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("checking its existence")
    request['api'] = 'qId'
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

    print("")
    print("deleting q")
    #create another q
    request = {'type':'client', 'api': 'qDestroy', 'params' :[2]}
    print("request {0}".format(request))
    print("response:-")
    sock.sendto(json.dumps(request).encode('utf-8'),('localhost',10001))
    print(sock.recvfrom(4096))

    print("")
    print("verifying deletion")
    request['api'] = 'qId'
    request['paras'] = [9]
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


