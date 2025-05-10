import Pyro5
from client import Client
from pyroObjects import Peer

files = {
        'TEST': "TestFile"
}

def main():
    deamon = Pyro5.api.Daemon()
    peerUri = deamon.register(Peer)

    print(peerUri)

    client = Client()
    client.start()
    deamon.requestLoop()
    

if __name__ == "__main__":
    main()