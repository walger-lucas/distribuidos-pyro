# saved as greeting-client.py
import threading
import Pyro5.api

class Client(threading.Thread):
    def run(self):
        while(True):
            uri = input("Informe a URI do servidor:").strip()
            filename = input("Informe o nome do arquivo:").strip()

            peer = Pyro5.api.Proxy(uri)
            print("Arquivo retornado: " + peer.getFile(filename))
            print("\n\n\n\n")