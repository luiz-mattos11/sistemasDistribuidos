import Cliente as cliente

class Lock:
    def __init__(self):
        self.id = 5
        self.c = cliente.Cliente()

    def run(self):
        while 1:
            passwdCode = input("Code: ")

            if (self.c.validate(self.id, passwdCode)):
                print("Authorized")
            else:
                print("Unauthorized")


Lock().run()
