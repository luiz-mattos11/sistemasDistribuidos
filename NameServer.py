import socket
import threadPool
import pickle
import json
import queue

MAX_THREADS = 100
THREAD_BLOCK = 10

class DNS:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 10001

        # Socket UDP
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.s.bind((self.ip, self.port))

        self.serverList = queue.Queue()

        self.threads = threadPool.tPool(self.getAddress, MAX_THREADS, THREAD_BLOCK)

        print("NameService is set up")

    def run(self):
        while True:
            data, addr = self.s.recvfrom(1024)

            print(addr)

            t = self.threads.getThread([data, addr])

            t.start()

    #Terminar getAddress
    def getAddress(self, data, address):
        message = self.loadMessage(data)

        print("MESSAGE: " + message)

        if message == "registerServer":
            print("Adding ", address, " server.")
            self.addQueueSv(address)
        else:
            svAddr = self.getServerAddress()

            self.sendToHost(address, svAddr)

        self.threads.repopulate()

    def getServerAddress(self):
        svAddr = self.removeQueueSv()

        if svAddr is not None:
            self.addQueueSv(svAddr)

        return svAddr

    def removeQueueSv(self):
        if self.serverList.empty():
            print("QUEUE EMPTY")
            return None

        return self.serverList.get()

    def addQueueSv(self, host):
        self.serverList.put(host)

    def convertJson(self, message):
        try:
            msg = json.dumps(message)
            return msg
        except:
            return message

    def loadJson(self, message):
        try:
            msg = json.loads(message)
            return msg
        except:
            return message

    def loadMessage(self, message):
        return self.loadJson(pickle.loads(message))

    def sendToHost(self, host, message):
        jsonMsg = self.convertJson(message)

        if jsonMsg is None:
            msgSerializada = pickle.dumps("ERROR!")
        else:
            msgSerializada = pickle.dumps(message)

        self.s.sendto(msgSerializada, host)

    def exit(self):
        self.s.close()

dns = DNS()
dns.run()
