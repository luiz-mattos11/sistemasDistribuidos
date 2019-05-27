import pickle
import socket
import json
import threadPool

MAX_THREADS = 100
THREAD_BLOCK = 10
DNS_IP = "127.0.0.1"
DNS_PORT = 10001

class Server:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 10002

        self.sock = None

        self.threads = threadPool.tPool(self.run, MAX_THREADS, THREAD_BLOCK)

        self.register()

        self.closeSocket()
    
        self.createSocketTCP()

        print("Server is set up.")

    def run(self, connection):
        while True:
            try:
                msg = self.getMessage(connection)
                print(msg)
                print(msg["lockId"])
                print(msg["code"])

                if (msg["lockId"] == 5 and msg["code"] == "15"):
                    connection.send(self.prepareMsg("Authorized"))
                else: connection.send(self.prepareMsg("Unauthorized"))
            except:
                print("End of connection")
                break


    def waitClient(self):
        while True:
            con, client = self.sock.accept()

            print("Cliente ", client, " conectado")
            t = self.threads.getThread([con])

            t.start()

    def getMessage(self, connection):
        serializedMsg = connection.recv(1024)
        msg = self.loadMessage(serializedMsg)
        return msg

    def verifyClient(self, data, address):
        message = self.loadMessage(data)

        print(message)


    def register(self):
        self.closeSocket()
        self.createSocketUDP()
        
        self.sendUDP((DNS_IP, DNS_PORT), self.prepareMsg("registerServer"))

    def sendTCP(self, serializedMsg):
        self.sock.send(serializedMsg)

    def createSocketTCP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                                  socket.SOCK_STREAM)  # TCP

        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)

    def closeSocket(self):
        try:
            self.sock.close()
        except:
            pass

    def createSocketUDP(self):
        self.sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)

        self.sock.bind((self.ip, self.port))

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

    def prepareMsg(self, msg):
        jsonMsg = self.convertJson(msg)

        serializedMsg = pickle.dumps(jsonMsg)

        return serializedMsg

    def sendUDP(self, host, serializedMessage):
        self.sock.sendto(serializedMessage, host)

sv = Server()
sv.waitClient()