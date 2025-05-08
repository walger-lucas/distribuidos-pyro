import Pyro5.api

@Pyro5.api.expose
class GreetingMaker(object):
    def get_fortune(self, name):
        return "Hello, {0}. Here is your fortune message:\n" \
               "Tomorrow's lucky number is 12345678.".format(name)

daemon = Pyro5.server.Daemon()         # make a Pyro daemon
try:
    ns = Pyro5.api.locate_ns() 
except:
    print("INICIALIZE O SERVIÇO DE NOMES!!!")
    exit()
uri = daemon.register(GreetingMaker)   # register the greeting maker as a Pyro object
ns.register("example.greeting", uri)   # register the object with a name in the name server

print("Ready.")
daemon.requestLoop() 